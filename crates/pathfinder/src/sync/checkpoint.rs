#![allow(dead_code, unused_variables)]
use std::collections::HashSet;
use std::num::NonZeroUsize;

use anyhow::Context;
use futures::{Stream, StreamExt, TryStreamExt};
use p2p::sync::client::conv::TryFromDto;
use p2p::sync::client::peer_agnostic::traits::{
    BlockClient,
    ClassStream,
    EventStream,
    HeaderStream,
    StateDiffStream,
    StreamItem,
    TransactionStream,
};
use p2p::sync::client::types::{ClassDefinition, EventsForBlockByTransaction, TransactionData};
use p2p::PeerData;
use p2p_proto::common::BlockNumberOrHash;
use p2p_proto::sync::common::{Direction, Iteration};
use p2p_proto::sync::transaction::{
    TransactionWithReceipt,
    TransactionsRequest,
    TransactionsResponse,
};
use pathfinder_block_commitments::calculate_transaction_commitment;
use pathfinder_block_hashes::BlockHashDb;
use pathfinder_common::prelude::*;
use pathfinder_common::receipt::Receipt;
use pathfinder_common::state_update::StateUpdateData;
use pathfinder_common::transaction::{Transaction, TransactionVariant};
use pathfinder_ethereum::EthereumStateUpdate;
use pathfinder_storage::Storage;
use primitive_types::H160;
use serde_json::de;
use starknet_gateway_client::{BlockId, Client, GatewayApi};
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::sync::error::SyncError;
use crate::sync::stream::{InfallibleSource, Source, SyncReceiver, SyncResult};
use crate::sync::{class_definitions, events, headers, state_updates, transactions};

/// Provides P2P sync capability for blocks secured by L1.
#[derive(Clone)]
pub struct Sync<P, G> {
    pub storage: Storage,
    pub p2p: P,
    // TODO: merge these two inside the client.
    pub eth_client: pathfinder_ethereum::EthereumClient,
    pub eth_address: H160,
    pub fgw_client: G,
    pub chain_id: ChainId,
    pub public_key: PublicKey,
    pub verify_tree_hashes: bool,
    pub compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    pub blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    pub block_hash_db: Option<pathfinder_block_hashes::BlockHashDb>,
}

impl<P, G> Sync<P, G>
where
    P: ClassStream
        + EventStream
        + HeaderStream
        + StateDiffStream
        + TransactionStream
        + Clone
        + Send
        + 'static,
    G: GatewayApi + Clone + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Storage,
        p2p: P,
        ethereum: (pathfinder_ethereum::EthereumClient, H160),
        fgw_client: G,
        chain_id: ChainId,
        public_key: PublicKey,
        l1_anchor_override: Option<EthereumStateUpdate>,
        verify_tree_hashes: bool,
        compiler_resource_limits: pathfinder_compiler::ResourceLimits,
        blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
        block_hash_db: Option<BlockHashDb>,
    ) -> Self {
        Self {
            storage,
            p2p,
            eth_client: ethereum.0,
            eth_address: ethereum.1,
            fgw_client,
            chain_id,
            public_key,
            verify_tree_hashes,
            compiler_resource_limits,
            blockifier_libfuncs,
            block_hash_db,
        }
    }

    /// Syncs using p2p until the given Ethereum checkpoint.
    ///
    /// Returns the block number and its parent hash where tracking sync is
    /// expected to continue.
    pub async fn run(
        &self,
        checkpoint: EthereumStateUpdate,
    ) -> Result<(BlockNumber, BlockHash), SyncError> {
        let local_state = LocalState::from_db(self.storage.clone(), checkpoint)
            .await
            .context("Querying local state")?;

        // Ensure our local state is consistent with the L1 checkpoint.
        CheckpointAnalysis::analyse(&local_state, &checkpoint)
            .handle(self.storage.clone())
            .await
            .context("Analysing local storage against L1 checkpoint")?;

        // Persist checkpoint as new L1 anchor. This must be done first to protect
        // against an interrupted sync process. Subsequent syncs will use this
        // value to rollback against. Persisting it later would result in more data than
        // necessary being rolled back (potentially all data if the header sync process
        // is frequently interrupted), so this ensures sync will progress even
        // under bad conditions.
        let anchor = checkpoint;
        persist_anchor(self.storage.clone(), anchor)
            .await
            .context("Persisting new Ethereum anchor")?;

        let head = anchor.block_number;

        // Sync missing headers in reverse chronological order, from the new anchor to
        // genesis.
        self.sync_headers(anchor).await?;

        // Sync the rest of the data in chronological order.
        self.sync_transactions(head, self.chain_id).await?;
        self.sync_state_updates(head, self.verify_tree_hashes)
            .await?;
        self.sync_class_definitions(head).await?;
        self.sync_events(head).await?;

        let local_state = LocalState::from_db(self.storage.clone(), checkpoint)
            .await
            .context("Querying local state after checkpoint sync")?;
        let (next_block_number, last_block_hash) = local_state
            .latest_header
            .map(|(number, hash)| (number + 1, hash))
            .unwrap_or((BlockNumber::GENESIS, BlockHash::ZERO));

        Ok((next_block_number, last_block_hash))
    }

    /// Syncs all headers in reverse chronological order, from the anchor point
    /// back to genesis. Fills in any gaps left by previous header syncs.
    ///
    /// As sync goes backwards from a known L1 anchor block, this method can
    /// guarantee that all sync'd headers are secured by L1.
    ///
    /// No guarantees are made about any headers newer than the anchor.
    #[tracing::instrument(level = "debug", skip(self, anchor))]
    async fn sync_headers(&self, anchor: EthereumStateUpdate) -> Result<(), SyncError> {
        while let Some(gap) =
            headers::next_gap(self.storage.clone(), anchor.block_number, anchor.block_hash)
                .await
                .context("Finding next gap in header chain")?
        {
            tracing::info!(?gap, "Syncing headers");

            handle_header_stream(
                self.p2p.clone().header_stream(gap.tail, gap.head, true),
                gap.head(),
                self.chain_id,
                self.public_key,
                self.block_hash_db.clone(),
                self.storage.clone(),
            )
            .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sync_transactions(
        &self,
        stop: BlockNumber,
        chain_id: ChainId,
    ) -> Result<(), SyncError> {
        let Some(start) = transactions::next_missing(self.storage.clone(), stop)
            .await
            .context("Finding next block with missing transaction(s)")?
        else {
            return Ok(());
        };

        let transaction_stream = self.p2p.clone().transaction_stream(
            start,
            stop,
            transactions::counts_stream(
                self.storage.clone(),
                start,
                stop,
                NonZeroUsize::new(100).expect("100>0"),
            ),
        );

        handle_transaction_stream(transaction_stream, self.storage.clone(), chain_id, start)
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sync_state_updates(
        &self,
        stop: BlockNumber,
        verify_tree_hashes: bool,
    ) -> Result<(), SyncError> {
        let Some(start) = state_updates::next_missing(self.storage.clone(), stop)
            .await
            .context("Finding next missing state update")?
        else {
            return Ok(());
        };

        let stream = self.p2p.clone().state_diff_stream(
            start,
            stop,
            state_updates::state_diff_length_stream(
                self.storage.clone(),
                start,
                stop,
                NonZeroUsize::new(100).expect("100>0"),
            ),
        );

        handle_state_diff_stream(stream, self.storage.clone(), start, verify_tree_hashes).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sync_class_definitions(&self, stop: BlockNumber) -> Result<(), SyncError> {
        let Some(start) = class_definitions::next_missing(self.storage.clone(), stop)
            .await
            .context("Finding next block with missing class definition(s)")?
        else {
            return Ok(());
        };

        let class_stream = self.p2p.clone().class_stream(
            start,
            stop,
            class_definitions::declared_class_counts_stream(
                self.storage.clone(),
                start,
                stop,
                NonZeroUsize::new(100).expect("100>0"),
            ),
        );

        let expected_declarations =
            class_definitions::expected_declarations_stream(self.storage.clone(), start, stop);

        handle_class_stream(
            class_stream,
            self.storage.clone(),
            self.fgw_client.clone(),
            self.compiler_resource_limits,
            self.blockifier_libfuncs,
            expected_declarations,
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sync_events(&self, stop: BlockNumber) -> Result<(), SyncError> {
        let Some(start) = events::next_missing(self.storage.clone(), stop)
            .context("Finding next block with missing events")?
        else {
            return Ok(());
        };

        let event_stream = self.p2p.clone().event_stream(
            start,
            stop,
            events::counts_stream(
                self.storage.clone(),
                start,
                stop,
                NonZeroUsize::new(100).expect("100>0"),
            ),
        );

        handle_event_stream(event_stream, self.storage.clone()).await?;

        Ok(())
    }
}

async fn handle_header_stream(
    stream: impl Stream<Item = PeerData<SignedBlockHeader>> + Send + 'static,
    head: (BlockNumber, BlockHash),
    chain_id: ChainId,
    public_key: PublicKey,
    block_hash_db: Option<pathfinder_block_hashes::BlockHashDb>,
    storage: Storage,
) -> Result<(), SyncError> {
    InfallibleSource::from_stream(stream)
        .spawn()
        .pipe(headers::BackwardContinuity::new(head.0, head.1), 10)
        .pipe(
            headers::VerifyHashAndSignature::new(chain_id, public_key, block_hash_db),
            10,
        )
        .try_chunks(1000, 10)
        .pipe(
            headers::Persist {
                connection: storage.connection().context("Creating db connection")?,
            },
            10,
        )
        .into_stream()
        .inspect_ok(|x| tracing::debug!(tail=%x.data, "Headers chunk synced"))
        .try_fold((), |_, _| std::future::ready(Ok(())))
        .await
}

async fn handle_transaction_stream(
    stream: impl Stream<Item = StreamItem<(TransactionData, BlockNumber)>> + Send + 'static,
    storage: Storage,
    chain_id: ChainId,
    start: BlockNumber,
) -> Result<(), SyncError> {
    Source::from_stream(stream.map_err(Into::into))
        .spawn()
        .pipe(
            transactions::FetchCommitmentFromDb::new(storage.connection()?),
            10,
        )
        .pipe(transactions::CalculateHashes(chain_id), 10)
        .pipe(transactions::VerifyCommitment, 10)
        .pipe(transactions::Store::new(storage.connection()?, start), 10)
        .into_stream()
        .inspect_ok(|x| tracing::debug!(tail=%x.data, "Transactions chunk synced"))
        .try_fold((), |_, _| std::future::ready(Ok(())))
        .await
}

async fn handle_state_diff_stream(
    stream: impl Stream<Item = StreamItem<(StateUpdateData, BlockNumber)>> + Send + 'static,
    storage: Storage,
    start: BlockNumber,
    verify_tree_hashes: bool,
) -> Result<(), SyncError> {
    Source::from_stream(stream.map_err(Into::into))
        .spawn()
        .pipe(
            state_updates::FetchCommitmentFromDb::new(storage.connection()?),
            10,
        )
        .pipe(state_updates::VerifyCommitment, 10)
        .into_stream()
        .try_chunks(1000)
        .map_err(|e| e.1)
        .and_then(|x| {
            state_updates::batch_update_starknet_state(storage.clone(), verify_tree_hashes, x)
        })
        .inspect_ok(|x| tracing::debug!(tail=%x.data, "State diffs chunk synced"))
        .try_fold((), |_, _| std::future::ready(Ok(())))
        .await
}

async fn handle_class_stream<SequencerClient: GatewayApi + Clone + Send + 'static>(
    class_definitions: impl Stream<Item = StreamItem<ClassDefinition>> + Send + 'static,
    storage: Storage,
    fgw: SequencerClient,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    expected_declarations: impl Stream<Item = anyhow::Result<(BlockNumber, HashSet<ClassHash>)>>
        + Send
        + 'static,
) -> Result<(), SyncError> {
    // Increasing the chunk size above num cpus improves performance even more.
    let chunk_size = std::thread::available_parallelism()
        .context("Getting available parallelism")?
        .get()
        * 8;

    let classes_with_hashes = class_definitions
        .map_err(Into::into)
        .and_then(class_definitions::verify_layout)
        .try_chunks(chunk_size)
        .map_err(|e| e.1)
        .and_then(class_definitions::verify_hash)
        .boxed();

    class_definitions::verify_declared_at(expected_declarations.boxed(), classes_with_hashes)
        .try_chunks(chunk_size)
        .map_err(|e| e.1)
        .and_then(|x| {
            class_definitions::compile_sierra_to_casm_or_fetch(
                x,
                fgw.clone(),
                tokio::runtime::Handle::current(),
                compiler_resource_limits,
                blockifier_libfuncs,
            )
        })
        .and_then(|x| class_definitions::persist(storage.clone(), x))
        .inspect_ok(|x| tracing::info!(tail=%x, "Class definitions chunk synced"))
        .try_fold((), |_, _| std::future::ready(Ok(())))
        .await
}

async fn handle_event_stream(
    stream: impl Stream<Item = StreamItem<EventsForBlockByTransaction>>,
    storage: Storage,
) -> Result<(), SyncError> {
    stream
        .map_err(Into::into)
        .and_then(|x| events::verify_commitment(x, storage.clone()))
        .try_chunks(100)
        .map_err(|e| e.1)
        .and_then(|x| events::persist(storage.clone(), x))
        .inspect_ok(|x| tracing::debug!(tail=%x, "Events chunk synced"))
        // Drive stream to completion.
        .try_fold((), |_, _| std::future::ready(Ok(())))
        .await
}

/// Performs [analysis](Self::analyse) of the [LocalState] by comparing it with
/// a given L1 checkpoint, and [handles](Self::handle) the result.
enum CheckpointAnalysis {
    /// The checkpoint hash does not match the local L1 anchor, indicating an
    /// inconsistency with the Ethereum source with the one used by the
    /// previous sync.
    HashMismatchWithAnchor {
        block: BlockNumber,
        checkpoint: BlockHash,
        anchor: BlockHash,
    },
    /// The checkpoint is older than the local anchor, indicating an
    /// inconsistency in the Ethereum source between this sync and the
    /// previous sync.
    PredatesAnchor {
        checkpoint: BlockNumber,
        anchor: BlockNumber,
    },
    /// The checkpoint exceeds the local chain. As such, the local chain should
    /// be rolled back to its anchor as we cannot be confident in any of the
    /// local data not verified by L1.
    ExceedsLocalChain {
        local: BlockNumber,
        checkpoint: BlockNumber,
        anchor: Option<BlockNumber>,
    },
    /// The checkpoint hash does not match the local chain data. The local chain
    /// should be rolled back to its anchor.
    HashMismatchWithLocalChain {
        block: BlockNumber,
        local: BlockHash,
        checkpoint: BlockHash,
        anchor: Option<BlockNumber>,
    },
    /// Local data is consistent with the checkpoint, no action required.
    Consistent,
}

impl CheckpointAnalysis {
    /// Analyse [LocalState] by checking it for consistency against the given L1
    /// checkpoint.
    ///
    /// For more information on the potential inconsistencies see the
    /// [CheckpointAnalysis] variants.
    fn analyse(local_state: &LocalState, checkpoint: &EthereumStateUpdate) -> CheckpointAnalysis {
        // Checkpoint is older than or inconsistent with our local L1 anchor.
        if let Some(anchor) = &local_state.anchor {
            if checkpoint.block_number < anchor.block_number {
                return CheckpointAnalysis::PredatesAnchor {
                    checkpoint: checkpoint.block_number,
                    anchor: anchor.block_number,
                };
            }
            if checkpoint.block_number == anchor.block_number
                && checkpoint.block_hash != anchor.block_hash
            {
                return CheckpointAnalysis::HashMismatchWithAnchor {
                    block: anchor.block_number,
                    checkpoint: checkpoint.block_hash,
                    anchor: anchor.block_hash,
                };
            }
        }

        // Is local data not secured by an anchor potentially invalid?
        if let Some(latest) = local_state.latest_header {
            if checkpoint.block_number > latest.0 {
                return CheckpointAnalysis::ExceedsLocalChain {
                    local: latest.0,
                    checkpoint: checkpoint.block_number,
                    anchor: local_state.anchor.as_ref().map(|x| x.block_number),
                };
            }
            if let Some((_, hash)) = local_state.checkpoint {
                if hash != checkpoint.block_hash {
                    return CheckpointAnalysis::HashMismatchWithLocalChain {
                        block: checkpoint.block_number,
                        local: hash,
                        checkpoint: checkpoint.block_hash,
                        anchor: local_state.anchor.as_ref().map(|x| x.block_number),
                    };
                }
            }
        }

        CheckpointAnalysis::Consistent
    }

    /// Handles the [checkpoint analysis](Self::analyse) [result](Self).
    ///
    /// Returns an error for [PredatesAnchor](Self::PredatesAnchor) and
    /// [HashMismatchWithAnchor](Self::HashMismatchWithAnchor) since these
    /// indicate an inconsistency with the Ethereum source - making all data
    /// suspect.
    ///
    /// Rolls back local state to the anchor for
    /// [ExceedsLocalChain](Self::ExceedsLocalChain) and
    /// [HashMismatchWithLocalChain](Self::HashMismatchWithLocalChain)
    /// conditions.
    ///
    /// Does nothing for [Consistent](Self::Consistent). This leaves any
    /// insecure local data intact. Always rolling back to the L1 anchor
    /// would result in a poor user experience if restarting frequently as each
    /// restart would purge new data.
    async fn handle(self, storage: Storage) -> anyhow::Result<()> {
        match self {
            CheckpointAnalysis::HashMismatchWithAnchor {
                block,
                checkpoint,
                anchor,
            } => {
                tracing::error!(
                    %block, %checkpoint, %anchor,
                    "Ethereum checkpoint's hash did not match the local Ethereum anchor. This indicates a serious inconsistency in the Ethereum source used by this sync and the previous sync."
                );
                anyhow::bail!("Ethereum checkpoint hash did not match local anchor.");
            }
            CheckpointAnalysis::PredatesAnchor { checkpoint, anchor } => {
                // TODO: or consider this valid. If so, then we should continue sync but use the
                // local anchor instead of the checkpoint.
                tracing::error!(
                    %checkpoint, %anchor,
                    "Ethereum checkpoint is older than the local anchor. This indicates a serious inconsistency in the Ethereum source used by this sync and the previous sync."
                );
                anyhow::bail!("Ethereum checkpoint is older than the local anchor.");
            }
            CheckpointAnalysis::ExceedsLocalChain {
                local,
                checkpoint,
                anchor,
            } => {
                tracing::info!(
                    %local, anchor=%anchor.unwrap_or_default(), %checkpoint,
                    "Rolling back local chain to latest anchor point. Local data is potentially invalid as the Ethereum checkpoint is newer the local chain."
                );

                rollback_to_anchor(storage, local, anchor)
                    .await
                    .context("Rolling back chain state to L1 anchor")?;
            }
            CheckpointAnalysis::HashMismatchWithLocalChain {
                block,
                local,
                checkpoint,
                anchor,
            } => {
                tracing::info!(
                    %block, %local, %checkpoint, ?anchor,
                    "Rolling back local chain to latest anchor point. Local data is invalid as it did not match the Ethereum checkpoint's hash."
                );

                rollback_to_anchor(storage, block, anchor)
                    .await
                    .context("Rolling back chain state to L1 anchor")?;
            }
            CheckpointAnalysis::Consistent => {
                tracing::info!("Ethereum checkpoint is consistent with local data");
            }
        };

        Ok(())
    }
}

struct LocalState {
    /// The highest header in our local storage.
    latest_header: Option<(BlockNumber, BlockHash)>,
    /// The highest L1 state update __in our local storage__.
    ///
    /// An L1 state update is Starknet's block number, hash and state root as
    /// recorded on Ethereum.
    anchor: Option<EthereumStateUpdate>,
    /// The highest L1 state update __fetched from Ethereum at the moment__.
    checkpoint: Option<(BlockNumber, BlockHash)>,
}

impl LocalState {
    async fn from_db(storage: Storage, checkpoint: EthereumStateUpdate) -> anyhow::Result<Self> {
        // TODO: this should include header gaps.
        util::task::spawn_blocking(move |_| {
            let mut db = storage
                .connection()
                .context("Creating database connection")?;
            let db = db.transaction().context("Creating database transaction")?;

            let latest_header = db
                .block_id(pathfinder_common::BlockId::Latest)
                .context("Querying latest header")?;

            let checkpoint = db
                .block_id(checkpoint.block_number.into())
                .context("Querying checkpoint header")?;

            let anchor = db.latest_l1_state().context("Querying latest L1 anchor")?;

            Ok(LocalState {
                latest_header,
                checkpoint,
                anchor,
            })
        })
        .await
        .context("Joining blocking task")?
    }
}

/// Rolls back local chain-state until the given anchor point, making it the tip
/// of the local chain. If this is ['None'] then all data will be rolled back.
async fn rollback_to_anchor(
    storage: Storage,
    local: BlockNumber,
    anchor: Option<BlockNumber>,
) -> anyhow::Result<()> {
    util::task::spawn_blocking(move |_| {
        tracing::info!(%local, ?anchor, "Rolling back storage to anchor point");

        let last_block_to_remove = anchor.map(|n| n + 1).unwrap_or_default();
        let mut head = local;

        let mut db = storage
            .connection()
            .context("Creating database connection")?;
        let transaction = db.transaction().context("Create database transaction")?;

        // TODO: roll back Merkle tree state once we're updating that

        while head >= last_block_to_remove {
            transaction
                .purge_block(head)
                .with_context(|| format!("Purging block {head} from database"))?;

            // No further blocks to purge if we just purged genesis.
            if head == BlockNumber::GENESIS {
                break;
            }

            head -= 1;
        }

        transaction
            .reset_in_memory_state(head)
            .context("Resetting in-memory DB state after reorg")?;

        transaction.commit().context("Committing transaction")?;

        Ok(())
    })
    .await
    .context("Joining blocking task")?
}

async fn persist_anchor(storage: Storage, anchor: EthereumStateUpdate) -> anyhow::Result<()> {
    util::task::spawn_blocking(move |_| {
        let mut db = storage
            .connection()
            .context("Creating database connection")?;
        let db = db.transaction().context("Creating database transaction")?;
        db.upsert_l1_state(&anchor).context("Inserting anchor")?;
        // TODO: this is a bit dodgy, but is used by the sync process. However it
        // destroys some RPC assumptions which we should be aware of.
        db.update_l1_l2_pointer(Some(anchor.block_number))
            .context("Updating L1-L2 pointer")?;
        db.commit().context("Committing database transaction")?;
        Ok(())
    })
    .await
    .context("Joining blocking task")?
}

#[cfg(test)]
mod tests {
    use tokio::task::spawn_blocking;

    use super::*;

    mod handle_header_stream {
        use assert_matches::assert_matches;
        use futures::stream;
        use pathfinder_common::prelude::*;
        use pathfinder_common::public_key;
        use pathfinder_storage::StorageBuilder;
        use rstest::rstest;
        use serde::Deserialize;
        use serde_with::{serde_as, DisplayFromStr};

        use super::*;
        use crate::sync::tests::generate_fake_blocks;

        struct Setup {
            pub streamed_headers: Vec<PeerData<SignedBlockHeader>>,
            pub expected_headers: Vec<SignedBlockHeader>,
            pub storage: Storage,
            pub head: (BlockNumber, BlockHash),
            pub public_key: PublicKey,
            pub block_hash_db: Option<pathfinder_block_hashes::BlockHashDb>,
        }

        #[serde_as]
        #[derive(Clone, Debug, Deserialize)]
        pub struct Fixture {
            pub block_hash: BlockHash,
            pub block_number: BlockNumber,
            pub parent_block_hash: BlockHash,
            pub sequencer_address: SequencerAddress,
            pub state_root: StateCommitment,
            pub timestamp: BlockTimestamp,
            pub transaction_commitment: TransactionCommitment,
            pub transaction_count: usize,
            pub event_commitment: EventCommitment,
            pub event_count: usize,
            pub signature: [BlockCommitmentSignatureElem; 2],
            pub state_diff_commitment: StateDiffCommitment,
            pub state_diff_length: u64,
            pub receipt_commitment: ReceiptCommitment,
            pub starknet_version: String,
            pub eth_l1_gas_price: GasPrice,
        }

        impl From<Fixture> for SignedBlockHeader {
            fn from(dto: Fixture) -> Self {
                let starknet_version: StarknetVersion = dto.starknet_version.parse().unwrap();
                Self {
                    header: BlockHeader {
                        hash: dto.block_hash,
                        number: dto.block_number,
                        parent_hash: dto.parent_block_hash,
                        sequencer_address: dto.sequencer_address,
                        state_commitment: dto.state_root,
                        timestamp: dto.timestamp,
                        transaction_commitment: dto.transaction_commitment,
                        transaction_count: dto.transaction_count,
                        event_commitment: dto.event_commitment,
                        event_count: dto.event_count,
                        state_diff_commitment: dto.state_diff_commitment,
                        state_diff_length: dto.state_diff_length,
                        receipt_commitment: dto.receipt_commitment,
                        starknet_version,
                        eth_l1_gas_price: dto.eth_l1_gas_price,
                        eth_l1_data_gas_price: GasPrice(1),
                        strk_l1_gas_price: GasPrice(0),
                        strk_l1_data_gas_price: GasPrice(1),
                        eth_l2_gas_price: GasPrice(0),
                        strk_l2_gas_price: GasPrice(0),
                        l1_da_mode: L1DataAvailabilityMode::Calldata,
                    },
                    signature: BlockCommitmentSignature {
                        r: dto.signature[0],
                        s: dto.signature[1],
                    },
                }
            }
        }

        fn setup_from_fixture() -> Setup {
            let expected_headers =
                serde_json::from_str::<Vec<Fixture>>(include_str!("fixtures/sepolia_headers.json"))
                    .unwrap()
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<SignedBlockHeader>>();

            let hdr = &expected_headers.last().unwrap().header;
            Setup {
                head: (hdr.number, hdr.hash),
                streamed_headers: expected_headers
                    .iter()
                    .rev()
                    .cloned()
                    .map(PeerData::for_tests)
                    .collect::<Vec<_>>(),
                expected_headers,
                storage: StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
                    pathfinder_storage::TriePruneMode::Archive,
                    std::num::NonZeroU32::new(5).unwrap(),
                )
                .unwrap(),
                // https://alpha-sepolia.starknet.io/feeder_gateway/get_public_key
                public_key: public_key!(
                    "0x1252b6bce1351844c677869c6327e80eae1535755b611c66b8f46e595b40eea"
                ),
                block_hash_db: Some(pathfinder_block_hashes::BlockHashDb::new(
                    pathfinder_common::Chain::SepoliaTestnet,
                )),
            }
        }

        fn setup_from_fake(num_blocks: usize) -> Setup {
            let (public_key, blocks) = generate_fake_blocks(num_blocks);
            let expected_headers = blocks.into_iter().map(|b| b.header).collect::<Vec<_>>();
            let hdr = &expected_headers.last().unwrap().header;

            Setup {
                head: (hdr.number, hdr.hash),
                streamed_headers: expected_headers
                    .iter()
                    .rev()
                    .cloned()
                    .map(PeerData::for_tests)
                    .collect::<Vec<_>>(),
                expected_headers,
                storage: StorageBuilder::in_tempdir().unwrap(),
                public_key,
                block_hash_db: None,
            }
        }

        // These two cases are an implicit verification that [`storage::fake::generate`]
        // is just good enough for tests.
        #[rstest]
        #[case::from_fixture(setup_from_fixture())]
        #[case::from_fake(setup_from_fake(10))]
        #[test_log::test(tokio::test)]
        async fn happy_path(#[case] setup: Setup) {
            let Setup {
                streamed_headers,
                expected_headers,
                storage,
                head,
                public_key,
                block_hash_db,
            } = setup;

            handle_header_stream(
                stream::iter(streamed_headers),
                head,
                ChainId::SEPOLIA_TESTNET,
                public_key,
                block_hash_db,
                storage.clone(),
            )
            .await
            .unwrap();

            let actual_headers = tokio::task::spawn_blocking(move || {
                let mut conn = storage.connection().unwrap();
                let db = conn.transaction().unwrap();
                (0..=head.0.get())
                    .map(|n| {
                        let block_number = BlockNumber::new_or_panic(n);
                        let block_id = block_number.into();
                        SignedBlockHeader {
                            header: db.block_header(block_id).unwrap().unwrap(),
                            signature: db.signature(block_id).unwrap().unwrap(),
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap();

            pretty_assertions_sorted::assert_eq!(expected_headers, actual_headers);
        }

        #[tokio::test]
        async fn discontinuity() {
            let Setup {
                mut streamed_headers,
                storage,
                head,
                public_key,
                ..
            } = setup_from_fixture();

            streamed_headers.last_mut().unwrap().data.header.number = BlockNumber::new_or_panic(3);

            assert_matches!(
                handle_header_stream(
                    stream::iter(streamed_headers),
                    head,
                    ChainId::SEPOLIA_TESTNET,
                    public_key,
                    Some(pathfinder_block_hashes::BlockHashDb::new(
                        pathfinder_common::Chain::SepoliaTestnet
                    )),
                    storage.clone(),
                )
                .await,
                Err(SyncError::Discontinuity(_))
            );
        }

        #[tokio::test]
        async fn bad_hash() {
            let Setup {
                streamed_headers,
                storage,
                head,
                public_key,
                ..
            } = setup_from_fixture();

            assert_matches!(
                handle_header_stream(
                    stream::iter(streamed_headers),
                    head,
                    // Causes mismatches for all block hashes because setup assumes Sepolia
                    ChainId::MAINNET,
                    public_key,
                    None,
                    storage.clone(),
                )
                .await,
                Err(SyncError::BadBlockHash(_))
            );
        }

        #[tokio::test]
        async fn bad_signature() {
            let Setup {
                streamed_headers,
                storage,
                head,
                block_hash_db,
                ..
            } = setup_from_fixture();

            assert_matches!(
                handle_header_stream(
                    stream::iter(streamed_headers),
                    head,
                    ChainId::SEPOLIA_TESTNET,
                    PublicKey::ZERO, // Invalid public key
                    block_hash_db,
                    storage.clone(),
                )
                .await,
                Err(SyncError::BadHeaderSignature(_))
            );
        }

        #[tokio::test]
        async fn db_failure() {
            let Setup {
                mut streamed_headers,
                storage,
                head,
                public_key,
                ..
            } = setup_from_fixture();

            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();
            let genesis = BlockHeader {
                number: BlockNumber::GENESIS,
                ..Default::default()
            };
            db.insert_block_header(&genesis).unwrap();
            db.commit().unwrap();

            assert_matches!(
                handle_header_stream(
                    stream::iter(streamed_headers),
                    head,
                    ChainId::SEPOLIA_TESTNET,
                    public_key,
                    Some(pathfinder_block_hashes::BlockHashDb::new(
                        pathfinder_common::Chain::SepoliaTestnet
                    )),
                    storage.clone(),
                )
                .await,
                Err(SyncError::Fatal(_))
            );
        }
    }

    mod handle_transaction_stream {
        use std::num::NonZeroU32;

        use assert_matches::assert_matches;
        use fake::{Dummy, Fake, Faker};
        use futures::stream;
        use p2p::libp2p::PeerId;
        use p2p::sync::client::types::TransactionData;
        use pathfinder_common::receipt::Receipt;
        use pathfinder_common::transaction::TransactionVariant;
        use pathfinder_common::{StarknetVersion, TransactionHash};
        use pathfinder_crypto::Felt;
        use pathfinder_storage::fake::{self as fake_storage, Block, Config};
        use pathfinder_storage::{StorageBuilder, TriePruneMode};

        use super::super::handle_transaction_stream;
        use super::*;

        struct Setup {
            pub streamed_transactions: Vec<StreamItem<(TransactionData, BlockNumber)>>,
            pub expected_transactions: Vec<Vec<(Transaction, Receipt)>>,
            pub storage: Storage,
        }

        fn setup(num_blocks: usize) -> Setup {
            setup_inner(
                num_blocks,
                Config {
                    calculate_transaction_commitment: Box::new(calculate_transaction_commitment),
                    ..Default::default()
                },
            )
        }

        fn setup_commitment_mismatch(num_blocks: usize) -> Setup {
            setup_inner(num_blocks, Default::default())
        }

        fn setup_inner(num_blocks: usize, config: Config) -> Setup {
            let blocks = fake_storage::generate::with_config(num_blocks, config);
            let only_headers = blocks
                .iter()
                .map(|block| Block {
                    header: block.header.clone(),
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            let storage = StorageBuilder::in_memory_with_trie_pruning_and_pool_size(
                TriePruneMode::Archive,
                NonZeroU32::new(5).unwrap(),
            )
            .unwrap();
            fake_storage::fill(&storage, &only_headers, None);

            let streamed_transactions = blocks
                .iter()
                .map(|block| {
                    anyhow::Result::Ok(PeerData::for_tests((
                        block
                            .transaction_data
                            .iter()
                            .map(|x| (x.0.clone(), x.1.clone().into()))
                            .collect::<Vec<_>>(),
                        block.header.header.number,
                    )))
                })
                .collect::<Vec<_>>();
            let expected_transactions = blocks
                .iter()
                .map(|block| {
                    block
                        .transaction_data
                        .iter()
                        .map(|x| (x.0.clone(), x.1.clone()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            Setup {
                streamed_transactions,
                expected_transactions,
                storage,
            }
        }

        #[tokio::test]
        async fn happy_path() {
            const NUM_BLOCKS: usize = 10;
            let Setup {
                streamed_transactions,
                expected_transactions,
                storage,
            } = setup(NUM_BLOCKS);

            handle_transaction_stream(
                stream::iter(streamed_transactions),
                storage.clone(),
                ChainId::SEPOLIA_TESTNET,
                BlockNumber::GENESIS,
            )
            .await
            .unwrap();

            let actual_transactions = tokio::task::spawn_blocking(move || {
                let mut conn = storage.connection().unwrap();
                let db_tx = conn.transaction().unwrap();
                (0..NUM_BLOCKS)
                    .map(|n| {
                        db_tx
                            .transactions_with_receipts_for_block(
                                BlockNumber::new_or_panic(n as u64).into(),
                            )
                            .unwrap()
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap();
            pretty_assertions_sorted::assert_eq!(expected_transactions, actual_transactions);
        }

        #[tokio::test]
        async fn transaction_mismatch() {
            let Setup {
                streamed_transactions,
                storage,
                ..
            } = setup(1);
            assert_matches!(
                handle_transaction_stream(
                    stream::iter(streamed_transactions),
                    storage.clone(),
                    // Causes mismatches for all transaction hashes because setup assumes
                    // ChainId::SEPOLIA_TESTNET
                    ChainId::MAINNET,
                    BlockNumber::GENESIS,
                )
                .await,
                Err(SyncError::BadTransactionHash(_))
            );
        }

        #[tokio::test]
        async fn commitment_mismatch() {
            let Setup {
                streamed_transactions,
                storage,
                ..
            } = setup_commitment_mismatch(1);
            assert_matches!(
                handle_transaction_stream(
                    stream::iter(streamed_transactions),
                    storage.clone(),
                    ChainId::SEPOLIA_TESTNET,
                    BlockNumber::GENESIS,
                )
                .await,
                Err(SyncError::TransactionCommitmentMismatch(_))
            );
        }

        #[tokio::test]
        async fn stream_failure() {
            assert_matches!(
                handle_transaction_stream(
                    stream::once(std::future::ready(Err(anyhow::anyhow!("")))),
                    StorageBuilder::in_memory().unwrap(),
                    ChainId::SEPOLIA_TESTNET,
                    BlockNumber::GENESIS,
                )
                .await,
                Err(SyncError::Fatal(_))
            );
        }

        #[tokio::test]
        async fn header_missing() {
            let Setup {
                streamed_transactions,
                ..
            } = setup(1);
            assert_matches!(
                handle_transaction_stream(
                    stream::iter(streamed_transactions),
                    StorageBuilder::in_memory().unwrap(),
                    ChainId::SEPOLIA_TESTNET,
                    BlockNumber::GENESIS,
                )
                .await,
                Err(SyncError::Fatal(_))
            );
        }
    }

    mod handle_state_diff_stream {
        use std::num::NonZeroU32;
        use std::path::PathBuf;

        use assert_matches::assert_matches;
        use fake::{Dummy, Fake, Faker};
        use futures::stream;
        use p2p::libp2p::PeerId;
        use pathfinder_common::state_update::{ContractClassUpdate, StateUpdateData};
        use pathfinder_common::transaction::DeployTransactionV0;
        use pathfinder_common::TransactionHash;
        use pathfinder_crypto::Felt;
        use pathfinder_merkle_tree::starknet_state::update_starknet_state;
        use pathfinder_storage::fake::{self as fake_storage, Block, Config};
        use pathfinder_storage::StorageBuilder;

        use super::super::handle_state_diff_stream;
        use super::*;

        struct Setup {
            pub streamed_state_diffs: Vec<StreamItem<(StateUpdateData, BlockNumber)>>,
            pub expected_state_diffs: Vec<StateUpdateData>,
            pub storage: Storage,
        }

        async fn setup(num_blocks: usize) -> Setup {
            tokio::task::spawn_blocking(move || {
                let blocks = fake_storage::generate::with_config(
                    num_blocks,
                    Config {
                        update_tries: Box::new(update_starknet_state),
                        ..Default::default()
                    },
                );

                let storage = pathfinder_storage::StorageBuilder::in_tempdir().unwrap();

                let headers_and_txns = blocks
                    .iter()
                    .map(|block| Block {
                        header: block.header.clone(),
                        transaction_data: block.transaction_data.clone(),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>();
                fake_storage::fill(&storage, &headers_and_txns, None);

                let streamed_state_diffs = blocks
                    .iter()
                    .map(|block| {
                        Result::<PeerData<_>, _>::Ok(PeerData::for_tests((
                            block.state_update.as_ref().unwrap().clone().into(),
                            block.header.header.number,
                        )))
                    })
                    .collect::<Vec<_>>();
                let expected_state_diffs = blocks
                    .iter()
                    .map(|block| block.state_update.as_ref().unwrap().clone().into())
                    .collect::<Vec<_>>();

                Setup {
                    streamed_state_diffs,
                    expected_state_diffs,
                    storage,
                }
            })
            .await
            .unwrap()
        }

        #[tokio::test]
        async fn happy_path() {
            const NUM_BLOCKS: usize = 10;
            let Setup {
                streamed_state_diffs,
                expected_state_diffs,
                storage,
            } = setup(NUM_BLOCKS).await;

            handle_state_diff_stream(
                stream::iter(streamed_state_diffs),
                storage.clone(),
                BlockNumber::GENESIS,
                true,
            )
            .await
            .unwrap();

            let actual_state_diffs = tokio::task::spawn_blocking(move || {
                let mut db = storage.connection().unwrap();
                let db = db.transaction().unwrap();
                (0..NUM_BLOCKS)
                    .map(|n| {
                        db.state_update(BlockNumber::new_or_panic(n as u64).into())
                            .unwrap()
                            .unwrap()
                            .into()
                    })
                    .collect::<Vec<StateUpdateData>>()
            })
            .await
            .unwrap();

            pretty_assertions_sorted::assert_eq!(expected_state_diffs, actual_state_diffs);
        }

        #[tokio::test]
        async fn commitment_mismatch() {
            let Setup {
                mut streamed_state_diffs,
                storage,
                ..
            } = setup(1).await;

            streamed_state_diffs[0]
                .as_mut()
                .unwrap()
                .data
                .0
                .declared_cairo_classes
                .insert(Faker.fake());

            assert_matches!(
                handle_state_diff_stream(
                    stream::iter(streamed_state_diffs),
                    storage,
                    BlockNumber::GENESIS,
                    false,
                )
                .await,
                Err(SyncError::StateDiffCommitmentMismatch(_))
            );
        }

        #[tokio::test]
        async fn stream_failure() {
            assert_matches!(
                handle_state_diff_stream(
                    stream::once(std::future::ready(Err(anyhow::anyhow!("")))),
                    StorageBuilder::in_memory().unwrap(),
                    BlockNumber::GENESIS,
                    false,
                )
                .await,
                Err(SyncError::Fatal(_))
            );
        }

        #[tokio::test]
        async fn header_missing() {
            let Setup {
                streamed_state_diffs,
                ..
            } = setup(1).await;
            assert_matches!(
                handle_state_diff_stream(
                    stream::iter(streamed_state_diffs),
                    StorageBuilder::in_memory().unwrap(),
                    BlockNumber::GENESIS,
                    false,
                )
                .await,
                Err(SyncError::Fatal(_))
            );
        }
    }

    mod handle_class_stream {
        use std::collections::HashMap;
        use std::future;

        use assert_matches::assert_matches;
        use fake::{Dummy, Fake, Faker};
        use futures::{stream, SinkExt};
        use p2p::libp2p::PeerId;
        use pathfinder_common::class_definition::{
            SerializedCairoDefinition,
            SerializedCasmDefinition,
            SerializedOpaqueClassDefinition,
            SerializedSierraDefinition,
        };
        use pathfinder_common::event::Event;
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::prelude::*;
        use pathfinder_common::transaction::TransactionVariant;
        use pathfinder_crypto::Felt;
        use pathfinder_storage::fake::{self as fake_storage, Block};
        use pathfinder_storage::StorageBuilder;
        use starknet_gateway_test_fixtures::class_definitions::{
            CAIRO_0_10_TUPLES_INTEGRATION as CAIRO,
            CAIRO_0_11_SIERRA as SIERRA0,
            CAIRO_1_1_0_BALANCE_CASM_JSON as CASM2,
            CAIRO_2_0_0_STACK_OVERFLOW as SIERRA2,
        };
        use starknet_gateway_types::error::SequencerError;

        use super::super::handle_class_stream;
        use super::*;

        const SIERRA0_HASH: SierraHash =
            sierra_hash!("0x04e70b19333ae94bd958625f7b61ce9eec631653597e68645e13780061b2136c");
        const SIERRA2_HASH: SierraHash =
            sierra_hash!("0x03dd9347d22f1ea2d5fbc7bd1f0860c6c334973499f9f1989fcb81bfff5191da");

        #[derive(Clone)]
        struct FakeFgw;

        #[async_trait::async_trait]
        impl GatewayApi for FakeFgw {
            async fn casm_by_hash(
                &self,
                _: ClassHash,
                _: BlockId,
            ) -> Result<SerializedCasmDefinition, SequencerError> {
                Ok(SerializedCasmDefinition::from_slice(CASM2))
            }
        }

        #[derive(Clone, Copy, Debug, Dummy)]
        struct DeclaredClass {
            pub block: BlockNumber,
            pub class: ClassHash,
        }

        #[derive(Clone, Debug)]
        struct DeclaredClasses(Vec<DeclaredClass>);

        impl DeclaredClasses {
            pub fn to_stream(
                &self,
            ) -> impl futures::Stream<Item = anyhow::Result<(BlockNumber, HashSet<ClassHash>)>>
            {
                let mut all = HashMap::<_, HashSet<ClassHash>>::new();
                self.0
                    .iter()
                    .copied()
                    .for_each(|DeclaredClass { block, class }| {
                        all.entry(block).or_default().insert(class);
                    });
                stream::iter(all.into_iter().map(Ok))
            }
        }

        impl<T> Dummy<T> for DeclaredClasses {
            fn dummy_with_rng<R: rand::Rng + ?Sized>(config: &T, rng: &mut R) -> Self {
                DeclaredClasses(fake::vec![DeclaredClass; 1..10])
            }
        }

        struct Setup {
            pub streamed_classes: Vec<Result<PeerData<ClassDefinition>, anyhow::Error>>,
            pub declared_classes: DeclaredClasses,
            pub expected_defs: HashMap<ClassHash, SerializedOpaqueClassDefinition>,
            pub storage: Storage,
        }

        /// The genesis block contains no declared classes
        async fn setup(expect_correct_class_hashes: bool) -> Setup {
            tokio::task::spawn_blocking(move || {
                let fake_block = |n, state_update| {
                    let mut block = Block::default();
                    block.header.header.number = BlockNumber::GENESIS + n;
                    block.header.header.hash = Faker.fake();
                    block.state_update = Some(state_update);
                    block
                };

                let (cairo_hash, sierra0_hash, sierra2_hash) = if expect_correct_class_hashes {
                    (
                        class_hash!(
                            "0x542460935cea188d21e752d8459d82d60497866aaad21f873cbb61621d34f7f"
                        ),
                        SIERRA0_HASH,
                        SIERRA2_HASH,
                    )
                } else {
                    Default::default()
                };

                let state_update1 = StateUpdate::default()
                    .with_declared_cairo_class(cairo_hash)
                    .with_declared_sierra_class(sierra0_hash, Default::default())
                    .with_declared_sierra_class(sierra2_hash, Default::default());
                let mut blocks = vec![
                    fake_block(0, Default::default()),
                    fake_block(1, state_update1),
                ];

                blocks[1].cairo_defs =
                    vec![(cairo_hash, SerializedCairoDefinition::from_slice(CAIRO))];
                blocks[1].sierra_defs = vec![
                    // Does not compile
                    (
                        sierra0_hash,
                        SerializedSierraDefinition::from_slice(SIERRA0),
                        Default::default(),
                        Default::default(),
                    ),
                    // Compiles just fine
                    (
                        sierra2_hash,
                        SerializedSierraDefinition::from_slice(SIERRA2),
                        Default::default(),
                        Default::default(),
                    ),
                ];

                let streamed_classes = vec![
                    Ok(PeerData::for_tests(ClassDefinition::Cairo {
                        block_number: BlockNumber::GENESIS + 1,
                        definition: SerializedCairoDefinition::from_slice(CAIRO),
                        hash: cairo_hash,
                    })),
                    Ok(PeerData::for_tests(ClassDefinition::Sierra {
                        block_number: BlockNumber::GENESIS + 1,
                        sierra_definition: SerializedSierraDefinition::from_slice(SIERRA0),
                        hash: sierra0_hash,
                    })),
                    Ok(PeerData::for_tests(ClassDefinition::Sierra {
                        block_number: BlockNumber::GENESIS + 1,
                        sierra_definition: SerializedSierraDefinition::from_slice(SIERRA2),
                        hash: sierra2_hash,
                    })),
                ];

                let declared_classes = DeclaredClasses(vec![
                    DeclaredClass {
                        block: BlockNumber::GENESIS + 1,
                        class: cairo_hash,
                    },
                    DeclaredClass {
                        block: BlockNumber::GENESIS + 1,
                        class: ClassHash(sierra0_hash.0),
                    },
                    DeclaredClass {
                        block: BlockNumber::GENESIS + 1,
                        class: ClassHash(sierra2_hash.0),
                    },
                ]);

                let expected_defs = [
                    (
                        cairo_hash,
                        SerializedOpaqueClassDefinition::from_slice(CAIRO),
                    ),
                    (
                        ClassHash(sierra0_hash.0),
                        SerializedOpaqueClassDefinition::from_slice(SIERRA0),
                    ),
                    (
                        ClassHash(sierra2_hash.0),
                        SerializedOpaqueClassDefinition::from_slice(SIERRA2),
                    ),
                ]
                .into();

                let storage = StorageBuilder::in_memory().unwrap();
                fake_storage::fill(&storage, &blocks, None);
                Setup {
                    streamed_classes,
                    declared_classes,
                    expected_defs,
                    storage,
                }
            })
            .await
            .unwrap()
        }

        #[tokio::test]
        async fn happy_path() {
            let Setup {
                streamed_classes,
                declared_classes,
                expected_defs,
                storage,
            } = setup(true).await;

            handle_class_stream(
                stream::iter(streamed_classes),
                storage.clone(),
                FakeFgw,
                pathfinder_compiler::ResourceLimits::for_test(),
                pathfinder_compiler::BlockifierLibfuncs::default(),
                declared_classes.to_stream(),
            )
            .await
            .unwrap();

            let actual_defs = tokio::task::spawn_blocking(move || {
                let mut db = storage.connection().unwrap();
                let db = db.transaction().unwrap();
                // Casm sanity checks
                assert_eq!(
                    db.casm_definition(ClassHash(SIERRA0_HASH.0))
                        .unwrap()
                        .unwrap(),
                    SerializedCasmDefinition::from_slice(CASM2)
                );
                assert!(serde_json::from_slice::<serde_json::Value>(
                    db.casm_definition(ClassHash(SIERRA2_HASH.0))
                        .unwrap()
                        .unwrap()
                        .as_slice()
                )
                .unwrap()["compiler_version"]
                    .is_string());
                // Cairo | sierra defs
                db.declared_classes_at((BlockNumber::GENESIS + 1).into())
                    .unwrap()
                    .unwrap()
                    .into_iter()
                    .map(|c| (c, db.class_definition(c).unwrap().unwrap()))
                    .collect::<HashMap<_, _>>()
            })
            .await
            .unwrap();

            assert_eq!(actual_defs, expected_defs);
        }

        #[rstest::rstest]
        #[case::cairo(ClassDefinition::Cairo {
            block_number: BlockNumber::GENESIS + 1,
            definition: Default::default(),
            hash: Default::default(),
        })]
        #[case::sierra(ClassDefinition::Sierra {
            block_number: BlockNumber::GENESIS + 1,
            sierra_definition: Default::default(),
            hash: Default::default(),
        })]
        #[tokio::test]
        async fn bad_layout(#[case] class: ClassDefinition) {
            let storage = StorageBuilder::in_memory().unwrap();
            let data = PeerData::for_tests(class);
            let expected_peer_id = data.peer;

            assert_matches!(
                    handle_class_stream(
                        stream::once(std::future::ready(Ok(data))),
                        storage,
                        FakeFgw,
                        pathfinder_compiler::ResourceLimits::for_test(),
                        pathfinder_compiler::BlockifierLibfuncs::default(),
                        Faker.fake::<DeclaredClasses>().to_stream(),
                    )
                    .await,
                    Err(SyncError::BadClassLayout(x)) => assert_eq!(x, expected_peer_id));
        }

        #[tokio::test]
        async fn unexpected_class() {
            let Setup {
                mut streamed_classes,
                declared_classes,
                storage,
                ..
            } = setup(true).await;

            match streamed_classes.last_mut().unwrap().as_mut().unwrap().data {
                ClassDefinition::Sierra {
                    ref mut block_number,
                    ..
                } => {
                    *block_number = BlockNumber::GENESIS + 2;
                }
                _ => unreachable!(),
            }
            let expected_peer_id = streamed_classes.last().unwrap().as_ref().unwrap().peer;

            assert_matches!(
                    handle_class_stream(
                        stream::iter(streamed_classes),
                        storage,
                        FakeFgw,
                        pathfinder_compiler::ResourceLimits::for_test(),
                        pathfinder_compiler::BlockifierLibfuncs::default(),
                        declared_classes.to_stream(),
                    )
                    .await,
                    Err(SyncError::UnexpectedClass(x)) => assert_eq!(x, expected_peer_id));
        }

        #[tokio::test]
        async fn stream_failure() {
            assert_matches!(
                handle_class_stream(
                    stream::once(std::future::ready(Err(anyhow::anyhow!("")))),
                    StorageBuilder::in_memory().unwrap(),
                    FakeFgw,
                    pathfinder_compiler::ResourceLimits::for_test(),
                    pathfinder_compiler::BlockifierLibfuncs::default(),
                    Faker.fake::<DeclaredClasses>().to_stream(),
                )
                .await,
                Err(SyncError::Fatal(_))
            );
        }
    }

    mod handle_event_stream {
        use assert_matches::assert_matches;
        use fake::{Fake, Faker};
        use futures::stream;
        use p2p::libp2p::PeerId;
        use pathfinder_block_commitments::calculate_event_commitment;
        use pathfinder_common::event::Event;
        use pathfinder_common::transaction::TransactionVariant;
        use pathfinder_common::{StarknetVersion, TransactionHash};
        use pathfinder_crypto::Felt;
        use pathfinder_storage::fake::{fill, Block, Config, EventCommitmentFn};
        use pathfinder_storage::{fake as fake_storage, StorageBuilder};

        use super::super::handle_event_stream;
        use super::*;

        type TransactionInfo = (TransactionHash, TransactionIndex);

        struct Setup {
            pub streamed_events: Vec<Result<PeerData<EventsForBlockByTransaction>, anyhow::Error>>,
            pub expected_events: Vec<Vec<(TransactionInfo, Vec<Event>)>>,
            pub storage: Storage,
        }

        fn setup(num_blocks: usize) -> Setup {
            setup_inner(
                num_blocks,
                Config {
                    calculate_event_commitment: Box::new(calculate_event_commitment),
                    ..Default::default()
                },
            )
        }

        fn setup_commitment_mismatch(num_blocks: usize) -> Setup {
            setup_inner(num_blocks, Default::default())
        }

        fn setup_inner(num_blocks: usize, config: Config) -> Setup {
            let blocks = fake_storage::generate::with_config(num_blocks, config);
            let without_events = blocks
                .iter()
                .cloned()
                .map(|mut block| {
                    block
                        .transaction_data
                        .iter_mut()
                        .for_each(|(_, _, e)| e.clear());
                    block
                })
                .collect::<Vec<_>>();
            let storage = StorageBuilder::in_memory().unwrap();
            fill(&storage, &without_events, None);

            let streamed_events = blocks
                .iter()
                .map(|block| {
                    Result::Ok(PeerData::for_tests((
                        block.header.header.number,
                        block
                            .transaction_data
                            .iter()
                            .map(|(tx, _, events)| (tx.hash, events.clone()))
                            .collect::<Vec<_>>(),
                    )))
                })
                .collect::<Vec<_>>();
            let expected_events = blocks
                .iter()
                .map(|block| {
                    block
                        .transaction_data
                        .iter()
                        .map(|x| ((x.0.hash, x.1.transaction_index), x.2.clone()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            Setup {
                streamed_events,
                expected_events,
                storage,
            }
        }

        #[tokio::test]
        async fn happy_path() {
            const NUM_BLOCKS: usize = 10;
            let Setup {
                streamed_events,
                expected_events,
                storage,
            } = setup(NUM_BLOCKS);

            handle_event_stream(stream::iter(streamed_events), storage.clone())
                .await
                .unwrap();

            let actual_events = tokio::task::spawn_blocking(move || {
                let mut conn = storage.connection().unwrap();
                let db_tx = conn.transaction().unwrap();
                (0..NUM_BLOCKS)
                    .map(|n| {
                        db_tx
                            .events_for_block(BlockNumber::new_or_panic(n as u64).into())
                            .unwrap()
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap();

            pretty_assertions_sorted::assert_eq!(expected_events, actual_events);
        }

        #[tokio::test]
        async fn commitment_mismatch() {
            const NUM_BLOCKS: usize = 1;
            let Setup {
                streamed_events,
                expected_events,
                storage,
            } = setup_commitment_mismatch(NUM_BLOCKS);
            let expected_peer_id = streamed_events[0].as_ref().unwrap().peer;

            assert_matches::assert_matches!(
                handle_event_stream(stream::iter(streamed_events), storage.clone())
                    .await
                    .unwrap_err(),
                SyncError::EventCommitmentMismatch(x) => assert_eq!(x, expected_peer_id)
            );
        }

        #[tokio::test]
        async fn stream_failure() {
            assert_matches::assert_matches!(
                handle_event_stream(
                    stream::once(std::future::ready(Err(anyhow::anyhow!("")))),
                    StorageBuilder::in_memory().unwrap()
                )
                .await
                .unwrap_err(),
                SyncError::Fatal(_)
            );
        }

        #[tokio::test]
        async fn header_missing() {
            assert_matches::assert_matches!(
                handle_event_stream(
                    stream::once(std::future::ready(Ok(Faker.fake()))),
                    StorageBuilder::in_memory().unwrap()
                )
                .await
                .unwrap_err(),
                SyncError::Fatal(_)
            );
        }
    }
}
