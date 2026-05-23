#![allow(dead_code, unused)]

use std::time::Duration;

use anyhow::Context;
use error::SyncError;
use futures::{pin_mut, Stream, StreamExt};
use p2p::sync::client::peer_agnostic::traits::{
    BlockClient,
    ClassStream,
    EventStream,
    HeaderStream,
    StateDiffStream,
    StreamItem,
    TransactionStream,
};
use p2p::PeerData;
use pathfinder_block_hashes::BlockHashDb;
use pathfinder_common::block_hash;
use pathfinder_common::prelude::*;
use pathfinder_ethereum::EthereumStateUpdate;
use pathfinder_storage::Transaction;
use primitive_types::H160;
use starknet_gateway_client::{Client as GatewayClient, GatewayApi};
use stream::ProcessStage;
use tokio::sync::watch::{self, Receiver};
use tokio_stream::wrappers::WatchStream;
use util::error::AnyhowExt;

use crate::state::RESET_DELAY_ON_FAILURE;

mod checkpoint;
mod class_definitions;
mod error;
mod events;
mod headers;
mod state_updates;
mod storage_adapters;
mod stream;
mod track;
mod transactions;

const CHECKPOINT_MARGIN: u64 = 10;

pub struct Sync<P, G> {
    pub storage: pathfinder_storage::Storage,
    pub p2p: P,
    pub eth_client: pathfinder_ethereum::EthereumClient,
    pub eth_address: H160,
    pub fgw_client: G,
    pub chain_id: ChainId,
    pub public_key: PublicKey,
    pub l1_checkpoint_override: Option<EthereumStateUpdate>,
    pub verify_tree_hashes: bool,
    pub compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    pub blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    pub block_hash_db: Option<BlockHashDb>,
}

impl<P, G> Sync<P, G>
where
    P: BlockClient
        + ClassStream
        + EventStream
        + HeaderStream
        + StateDiffStream
        + TransactionStream
        + Clone
        + Send
        + 'static,
    G: GatewayApi + Clone + Send + 'static,
{
    pub async fn run(self) -> anyhow::Result<()> {
        let (next, parent_hash) = self.checkpoint_sync().await?;

        self.track_sync(next, parent_hash).await
    }

    async fn handle_recoverable_error(&self, err: &error::SyncError) {
        // TODO
        tracing::debug!(%err, "Log and punish as appropriate");
    }

    /// Retry forever until a valid L1 checkpoint is retrieved
    ///
    /// ### Important
    ///
    /// We assume that the L1 endpoint is configured correctly and any L1 API
    /// errors are transient. We cannot proceed without a checkpoint, so we
    /// retry until we get one.
    async fn get_checkpoint(&self) -> pathfinder_ethereum::EthereumStateUpdate {
        if let Some(forced) = &self.l1_checkpoint_override {
            return *forced;
        }

        loop {
            match self.eth_client.get_starknet_state(&self.eth_address).await {
                Ok(latest) => return latest,
                Err(error) => {
                    tracing::warn!(%error, "Failed to get L1 checkpoint, retrying");
                    tokio::time::sleep(RESET_DELAY_ON_FAILURE);
                }
            }
        }
    }

    /// Run checkpoint sync until it completes successfully, and we are within
    /// some margin of the latest L1 block. Returns the next block number to
    /// sync and its parent hash.
    ///
    /// ### Important
    ///
    /// Sync is restarted on recoverable errors and only fatal errors (e.g.:
    /// database failure, runtime failure, etc.) cause this function to exit
    /// with an error.
    async fn checkpoint_sync(&self) -> anyhow::Result<(BlockNumber, BlockHash)> {
        let mut checkpoint = self.get_checkpoint().await;
        let from = (checkpoint.block_number, checkpoint.block_hash);

        tracing::info!(?from, "Checkpoint sync started");

        loop {
            let result = checkpoint::Sync {
                storage: self.storage.clone(),
                p2p: self.p2p.clone(),
                eth_client: self.eth_client.clone(),
                eth_address: self.eth_address,
                fgw_client: self.fgw_client.clone(),
                chain_id: self.chain_id,
                public_key: self.public_key,
                verify_tree_hashes: self.verify_tree_hashes,
                compiler_resource_limits: self.compiler_resource_limits,
                blockifier_libfuncs: self.blockifier_libfuncs,
                block_hash_db: self.block_hash_db.clone(),
            }
            .run(checkpoint)
            .await;

            // Handle the error
            let continue_from = match result {
                Ok(continue_from) => {
                    tracing::debug!(?continue_from, "Checkpoint sync complete");
                    continue_from
                }
                Err(SyncError::Fatal(mut error)) => {
                    tracing::error!(?error, "Stopping checkpoint sync");
                    return Err(error.take_or_deep_clone());
                }
                Err(error) => {
                    tracing::debug!(%error, "Restarting checkpoint sync");
                    self.handle_recoverable_error(&error).await;
                    continue;
                }
            };

            // Initial sync might take so long that the latest checkpoint is actually far
            // ahead again. Repeat until we are within some margin of L1.
            let latest_checkpoint = self.get_checkpoint().await;
            if checkpoint.block_number + CHECKPOINT_MARGIN < latest_checkpoint.block_number {
                checkpoint = latest_checkpoint;
                tracing::debug!(
                    local_checkpoint=%checkpoint.block_number, latest_checkpoint=%latest_checkpoint.block_number,
                    "Restarting checkpoint sync: L1 checkpoint has advanced"
                );
                continue;
            }

            break Ok(continue_from);
        }
    }

    /// Run the track sync forever, requires the number and parent hash of the
    /// first block to sync.
    ///
    /// ### Important
    ///
    /// Sync is restarted on recoverable errors and only fatal errors (e.g.:
    /// database failure, runtime failure, etc.) cause this function to exit
    /// with an error.
    async fn track_sync(
        &self,
        mut next: BlockNumber,
        mut parent_hash: BlockHash,
    ) -> anyhow::Result<()> {
        tracing::info!(next_block=%next, "Track sync started");

        loop {
            let mut result = track::Sync {
                latest: LatestStream::spawn(self.fgw_client.clone(), Duration::from_secs(2)),
                p2p: self.p2p.clone(),
                storage: self.storage.clone(),
                chain_id: self.chain_id,
                public_key: self.public_key,
                verify_tree_hashes: self.verify_tree_hashes,
                compiler_resource_limits: self.compiler_resource_limits,
                blockifier_libfuncs: self.blockifier_libfuncs,
                block_hash_db: self.block_hash_db.clone(),
            }
            .run(&mut next, &mut parent_hash, self.fgw_client.clone())
            .await;

            match result {
                Ok(_) => tracing::debug!("Restarting track sync: unexpected end of Block stream"),
                Err(SyncError::Fatal(mut error)) => {
                    tracing::error!(?error, "Stopping track sync");
                    return Err(error.take_or_deep_clone());
                }
                Err(error) => {
                    tracing::debug!(%error, "Restarting track sync");
                    self.handle_recoverable_error(&error).await;
                }
            }
        }
    }
}

struct LatestStream {
    rx: Receiver<(BlockNumber, BlockHash)>,
    stream: WatchStream<(BlockNumber, BlockHash)>,
}

impl Clone for LatestStream {
    fn clone(&self) -> Self {
        Self {
            // Keep the rx for the next clone
            rx: self.rx.clone(),
            // Create a new stream from the cloned rx, don't yield the initial value
            stream: WatchStream::from_changes(self.rx.clone()),
        }
    }
}

impl Stream for LatestStream {
    type Item = (BlockNumber, BlockHash);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let stream = &mut self.stream;
        pin_mut!(stream);
        stream.poll_next(cx)
    }
}

impl LatestStream {
    fn spawn<G>(fgw: G, head_poll_interval: Duration) -> Self
    where
        G: GatewayApi + Clone + Send + 'static,
    {
        // No buffer, for backpressure
        let (tx, rx) = watch::channel((BlockNumber::GENESIS, BlockHash::ZERO));

        util::task::spawn(async move {
            let mut interval = tokio::time::interval(head_poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;

                let Ok(latest) = fgw
                    .block_header(starknet_gateway_client::BlockId::Latest)
                    .await
                    .inspect_err(|e| tracing::debug!(error=%e, "Error requesting latest block ID"))
                else {
                    continue;
                };

                tracing::trace!(?latest, "LatestStream");

                if tx.is_closed() {
                    tracing::debug!("Channel closed, exiting");
                    break;
                }

                tx.send_if_modified(|current| {
                    if *current != latest {
                        tracing::info!(?latest, "LatestStream");
                        *current = latest;
                        true
                    } else {
                        false
                    }
                });
            }
        });

        Self {
            rx: rx.clone(),
            stream: WatchStream::from_changes(rx),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::ops::{Range, RangeInclusive};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use fake::{Fake, Faker};
    use futures::stream;
    use http::header;
    use p2p::libp2p::PeerId;
    use p2p::sync::client::types::{
        ClassDefinition,
        ClassDefinitionsError,
        EventsForBlockByTransaction,
        EventsResponseStreamFailure,
        Receipt as P2PReceipt,
        StateDiffsError,
        TransactionData,
    };
    use pathfinder_block_commitments::{
        calculate_event_commitment,
        calculate_receipt_commitment,
        calculate_transaction_commitment,
        compute_final_hash,
    };
    use pathfinder_common::class_definition::{
        SerializedCairoDefinition,
        SerializedCasmDefinition,
        SerializedOpaqueClassDefinition,
        SerializedSierraDefinition,
    };
    use pathfinder_common::event::Event;
    use pathfinder_common::prelude::*;
    use pathfinder_common::receipt::Receipt;
    use pathfinder_common::state_update::{self, StateUpdateData};
    use pathfinder_common::transaction::Transaction;
    use pathfinder_crypto::signature::ecdsa_sign;
    use pathfinder_crypto::Felt;
    use pathfinder_ethereum::EthereumClient;
    use pathfinder_merkle_tree::starknet_state::update_starknet_state;
    use pathfinder_storage::fake::{generate, Block, Config};
    use pathfinder_storage::{Storage, StorageBuilder};
    use rand::Rng;
    use rayon::iter::Rev;
    use rstest::rstest;
    use sha3::digest::consts::U6;
    use starknet_gateway_client::BlockId;
    use starknet_gateway_types::error::SequencerError;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(10);

    /// Generate a fake chain of blocks as in
    /// [`pathfinder_storage::fake::generate`] but with additional
    /// guarantees:
    /// - all commitments computed correctly
    /// - all block hashes computed correctly
    /// - all blocks signed with the same private key
    ///
    /// Returns: public key, generated blocks.
    pub fn generate_fake_blocks(num_blocks: usize) -> (PublicKey, Vec<Block>) {
        let private_key = Faker.fake();
        let public_key = PublicKey(pathfinder_crypto::signature::get_pk(private_key).unwrap());
        let blocks = generate::with_config(
            num_blocks,
            Config {
                calculate_block_hash: Box::new(compute_final_hash),
                sign_block_hash: Box::new(move |block_hash| ecdsa_sign(private_key, block_hash.0)),
                calculate_transaction_commitment: Box::new(calculate_transaction_commitment),
                calculate_receipt_commitment: Box::new(calculate_receipt_commitment),
                calculate_event_commitment: Box::new(calculate_event_commitment),
                update_tries: Box::new(update_starknet_state),
                ..Default::default()
            },
        );
        (public_key, blocks)
    }

    async fn sync_done_watch(
        mut last_event_rx: tokio::sync::mpsc::Receiver<()>,
        storage: Storage,
        expected_last: BlockNumber,
    ) {
        // Don't poll the DB until the last event is emitted from the fake P2P client
        last_event_rx.recv().await.unwrap();

        let mut interval = tokio::time::interval_at(
            // Give sync some slack to process the last event and commit the last block
            tokio::time::Instant::now() + Duration::from_millis(500),
            Duration::from_millis(200),
        );

        let mut start = std::time::Instant::now();

        loop {
            interval.tick().await;
            let storage = storage.clone();

            let done = tokio::task::spawn_blocking(move || {
                let mut db = storage.connection().unwrap();
                let db = db.transaction().unwrap();
                // We don't have to query the entire block, as tracking sync commits entire
                // blocks to the DB, so if the header is there, the block is there
                let header = db.block_header(expected_last.into()).unwrap();
                if let Some(header) = header {
                    if header.number == expected_last {
                        let after = start.elapsed();
                        tracing::info!(?after, "Sync done");
                        return true;
                    }
                }

                false
            })
            .await
            .unwrap();

            if done {
                break;
            }
        }
    }

    #[derive(Copy, Clone, Debug)]
    struct ErrorSetup {
        fatal_at: Option<BlockNumber>,
        expected_last_synced: LastSynced,
    }

    #[derive(Copy, Clone, Debug)]
    enum LastSynced {
        Full(BlockNumber),
        HeadersOnly(BlockNumber),
    }

    impl LastSynced {
        fn block_number(&self) -> BlockNumber {
            match self {
                LastSynced::Full(b) | LastSynced::HeadersOnly(b) => *b,
            }
        }

        fn is_full(&self) -> bool {
            matches!(self, LastSynced::Full(_))
        }
    }

    const ERROR_CONSUMED: u64 = u64::MAX;
    const CHECKPOINT_BLOCKS: u64 = 10;
    const TRACK_BLOCKS: u64 = CHECKPOINT_MARGIN - 1;
    const ALL_BLOCKS: u64 = CHECKPOINT_BLOCKS + TRACK_BLOCKS;
    const LAST_IN_CHECKPOINT: BlockNumber = BlockNumber::new_or_panic(CHECKPOINT_BLOCKS - 1);
    const LAST_IN_TRACK: BlockNumber = BlockNumber::new_or_panic(ALL_BLOCKS - 1);

    #[rstest]
    #[case::sync_restarts_after_recoverable_errors_and_succeeds(ErrorSetup {
        // Each sync stage will experience a recoverable error at random blocks
        fatal_at: None,
        // All blocks will be stored successfully
        expected_last_synced: LastSynced::Full(LAST_IN_TRACK),
    })]
    #[case::checkpoint_bails_after_fatal_error(ErrorSetup {
        fatal_at: Some(LAST_IN_CHECKPOINT),
        // All headers are stored but transactions fail
        expected_last_synced: LastSynced::HeadersOnly(LAST_IN_CHECKPOINT),
    })]
    #[case::track_bails_after_fatal_error(ErrorSetup {
        fatal_at: Some(LAST_IN_TRACK),
        // The last block is not stored
        expected_last_synced: LastSynced::Full(LAST_IN_TRACK - 1),
    })]
    #[test_log::test(tokio::test)]
    async fn sync(#[case] error_setup: ErrorSetup) {
        use futures::FutureExt;

        let (public_key, blocks) = generate_fake_blocks(ALL_BLOCKS as usize);
        let last_header = &blocks.last().unwrap().header.header;
        let last_checkpoint_header = &blocks[LAST_IN_CHECKPOINT.get() as usize].header.header;
        let storage = StorageBuilder::in_tempdir().unwrap();

        let expected_last_synced_block = error_setup.expected_last_synced.block_number();
        let expect_fully_synced_blocks = error_setup.expected_last_synced.is_full();

        let error_trigger = ErrorTrigger::new(error_setup.fatal_at);
        let (last_event_tx, mut last_event_rx) = tokio::sync::mpsc::channel(1);

        let sync = Sync {
            storage: storage.clone(),
            p2p: FakeP2PClient {
                blocks: blocks.clone(),
                error_trigger: error_trigger.clone(),
                storage: storage.clone(),
                last_event_tx,
            },
            // We use `l1_checkpoint_override` instead
            eth_client: EthereumClient::new("https://unused.com").unwrap(),
            eth_address: H160::zero(), // Unused
            fgw_client: FakeFgw {
                head: (last_header.number, last_header.hash),
            },
            chain_id: ChainId::SEPOLIA_TESTNET,
            public_key,
            l1_checkpoint_override: Some(EthereumStateUpdate {
                state_root: last_checkpoint_header.state_commitment,
                block_number: last_checkpoint_header.number,
                block_hash: last_checkpoint_header.hash,
            }),
            verify_tree_hashes: true,
            compiler_resource_limits: pathfinder_compiler::ResourceLimits::for_test(),
            blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs::default(),
            block_hash_db: None,
        };

        let sync_done = if error_setup.fatal_at.is_some() {
            // Sync will either bail on fatal error or time out
            std::future::pending().boxed()
        } else {
            // Successful sync never ends
            sync_done_watch(last_event_rx, storage.clone(), expected_last_synced_block).boxed()
        };

        let bail_early = tokio::select! {
            result = tokio::time::timeout(TIMEOUT, sync.run()) => match result {
                Ok(Ok(())) => unreachable!("Sync does not exit upon success, sync_done_watch should have been triggered"),
                Ok(Err(error)) => {
                    let unexpected_fatal = error_setup.fatal_at.is_none();
                    if unexpected_fatal {
                        tracing::debug!(?error, "Sync failed with an unexpected fatal error");
                    } else {
                        tracing::debug!(?error, "Sync failed with a fatal error");
                    }
                    unexpected_fatal
                },
                Err(_) => {
                    tracing::debug!("Test timed out");
                    true
                },
            },
            _ = sync_done => {
                tracing::debug!("Sync completion detected");
                false
            },
        };

        if bail_early {
            blocks.iter().for_each(|b| {
                tracing::error!(block=%b.header.header.number, state_update=?b.state_update.as_ref().unwrap());
            });
            return;
        }

        assert!(error_trigger.all_errors_triggered());

        let mut db = storage.connection().unwrap();
        let db = db.transaction().unwrap();
        for mut expected in blocks
            .into_iter()
            .take_while(|block| block.header.header.number <= expected_last_synced_block)
        {
            let block_number = expected.header.header.number;
            let block_id = block_number.into();
            let header = db.block_header(block_id).unwrap().unwrap();
            let signature = db.signature(block_id).unwrap().unwrap();

            pretty_assertions_sorted::assert_eq!(
                header,
                expected.header.header,
                "block {}",
                block_number
            );
            pretty_assertions_sorted::assert_eq!(
                signature,
                expected.header.signature,
                "block {}",
                block_number
            );

            if expect_fully_synced_blocks {
                let transaction_data = db.transaction_data_for_block(block_id).unwrap().unwrap();
                let state_update_data: StateUpdateData =
                    db.state_update(block_id).unwrap().unwrap().into();
                let declared = db.declared_classes_at(block_id).unwrap().unwrap();

                let mut cairo_defs = HashMap::new();
                let mut sierra_defs = HashMap::new();

                for class_hash in declared {
                    let class = db.class_definition(class_hash).unwrap().unwrap();
                    match db.casm_hash(class_hash).unwrap() {
                        Some(casm_hash) => {
                            let casm = db.casm_definition(class_hash).unwrap().unwrap();
                            sierra_defs.insert(SierraHash(class_hash.0), (class, casm));
                        }
                        None => {
                            cairo_defs.insert(class_hash, class);
                        }
                    }
                }

                pretty_assertions_sorted::assert_eq!(
                    header.state_diff_commitment,
                    expected.header.header.state_diff_commitment,
                    "block {}",
                    block_number
                );
                pretty_assertions_sorted::assert_eq!(
                    header.state_diff_length,
                    expected.header.header.state_diff_length,
                    "block {}",
                    block_number
                );
                pretty_assertions_sorted::assert_eq!(
                    transaction_data,
                    expected.transaction_data,
                    "block {}",
                    block_number
                );
                pretty_assertions_sorted::assert_eq!(
                    state_update_data,
                    expected.state_update.unwrap().into(),
                    "block {}",
                    block_number
                );
                pretty_assertions_sorted::assert_eq!(
                    cairo_defs,
                    expected
                        .cairo_defs
                        .into_iter()
                        .map(|(h, d)| (h, SerializedOpaqueClassDefinition::from(d)))
                        .collect::<HashMap<_, _>>(),
                    "block {}",
                    block_number
                );
                pretty_assertions_sorted::assert_eq!(
                    sierra_defs,
                    expected
                        .sierra_defs
                        .into_iter()
                        // All sierra fixtures are not compile-able
                        .map(|(h, s, _, _)| (
                            h,
                            (
                                SerializedOpaqueClassDefinition::from(s),
                                SerializedCasmDefinition::from_slice(
                                    starknet_gateway_test_fixtures::class_definitions::CAIRO_1_1_0_BALANCE_CASM_JSON
                                ),
                            )
                        ))
                        .collect::<HashMap<_, _>>(),
                    "block {}",
                    block_number
                );
            }
        }
    }

    #[derive(Clone)]
    struct FakeP2PClient {
        pub blocks: Vec<Block>,
        pub error_trigger: ErrorTrigger,
        pub storage: Storage,
        pub last_event_tx: tokio::sync::mpsc::Sender<()>,
    }

    #[derive(Clone)]
    enum ErrorTrigger {
        Recoverable(Arc<Vec<AtomicU64>>),
        Fatal(Arc<AtomicU64>),
    }

    impl ErrorTrigger {
        fn new(fatal_at: Option<BlockNumber>) -> Self {
            match fatal_at {
                Some(fatal_at) => Self::Fatal(Arc::new(AtomicU64::new(fatal_at.get()))),
                None => Self::Recoverable(Arc::new(
                    (0..=4)
                        .map(|_| AtomicU64::new((0..CHECKPOINT_BLOCKS).fake()))
                        .chain(
                            // The last block is always error free to ease checking for sync
                            // completion
                            (5..=9).map(|_| {
                                AtomicU64::new((CHECKPOINT_BLOCKS..ALL_BLOCKS - 1).fake())
                            }),
                        )
                        .collect(),
                )),
            }
        }

        fn fatal(&self, block: BlockNumber) -> bool {
            match self {
                Self::Fatal(at) => at
                    .compare_exchange(
                        block.get(),
                        ERROR_CONSUMED,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok(),
                Self::Recoverable { .. } => false,
            }
        }

        // Sync stages:
        // - 0: checkpoint, header
        // ...
        // - 4: checkpoint, event
        // - 5: track, header
        // ...
        // - 9: track, event
        fn recoverable(&self, block: BlockNumber, sync_stage: usize) -> bool {
            match self {
                Self::Fatal(_) => false,
                Self::Recoverable(triggers) => {
                    let at = &triggers[sync_stage];
                    at.compare_exchange(
                        block.get(),
                        ERROR_CONSUMED,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                }
            }
        }

        fn all_errors_triggered(&self) -> bool {
            match self {
                Self::Fatal(at) => at.load(Ordering::Relaxed) == ERROR_CONSUMED,
                Self::Recoverable(triggers) => triggers
                    .iter()
                    .all(|at| at.load(Ordering::Relaxed) == ERROR_CONSUMED),
            }
        }
    }

    impl FakeP2PClient {
        fn blocks<T, F>(
            mut self,
            start: BlockNumber,
            stop: BlockNumber,
            reverse: bool,
            map_fn: F,
        ) -> Vec<T>
        where
            F: FnMut(Block) -> T,
        {
            let mut blocks = self
                .blocks
                .into_iter()
                .filter_map(move |b| {
                    let n = b.header.header.number;
                    (n >= start && n <= stop).then_some(b)
                })
                .collect::<Vec<_>>();

            if reverse {
                blocks.reverse();
            }

            blocks.into_iter().map(map_fn).collect()
        }
    }

    impl HeaderStream for FakeP2PClient {
        fn header_stream(
            self,
            start: BlockNumber,
            stop: BlockNumber,
            reverse: bool,
        ) -> impl Stream<Item = PeerData<SignedBlockHeader>> + Send {
            let error_trigger = self.error_trigger.clone();

            stream::iter(self.blocks(start, stop, reverse, |mut b| {
                let block = b.header.header.number;

                if error_trigger.recoverable(block, 0) || error_trigger.recoverable(block, 5) {
                    tracing::debug!(%block,
                        "FakeP2PClient::header_stream triggering recoverable error at",
                    );
                    // This will cause discontinuity
                    b.header.header = Faker.fake();
                }

                PeerData::for_tests(b.header)
            }))
        }
    }

    impl TransactionStream for FakeP2PClient {
        fn transaction_stream(
            self,
            start: BlockNumber,
            stop: BlockNumber,
            _: impl Stream<Item = anyhow::Result<usize>> + Send + 'static,
        ) -> impl Stream<Item = StreamItem<(TransactionData, BlockNumber)>> + Send {
            let error_trigger = self.error_trigger.clone();

            stream::iter(self.blocks(start, stop, false, |mut b| {
                let block = b.header.header.number;

                if error_trigger.recoverable(block, 1) {
                    tracing::debug!(%block,
                        "FakeP2PClient::transaction_stream triggering recoverable error at",
                    );
                    // This will cause transaction commitment mismatch
                    b.transaction_data.pop();
                }

                if error_trigger.fatal(block) {
                    tracing::debug!(%block,
                        "FakeP2PClient::transaction_stream triggering fatal error at",
                    );
                    anyhow::bail!("Fatal error at block {block}");
                }

                Ok(PeerData::for_tests((
                    b.transaction_data
                        .into_iter()
                        .map(|(t, r, _)| (t, r.into()))
                        .collect(),
                    b.header.header.number,
                )))
            }))
        }
    }

    impl StateDiffStream for FakeP2PClient {
        fn state_diff_stream(
            self,
            start: BlockNumber,
            stop: BlockNumber,
            _: impl Stream<Item = anyhow::Result<usize>> + Send + 'static,
        ) -> impl Stream<Item = StreamItem<(StateUpdateData, BlockNumber)>> + Send {
            let error_trigger = self.error_trigger.clone();

            stream::iter(self.blocks(start, stop, false, |mut b| {
                let block = b.header.header.number;

                if error_trigger.recoverable(block, 2) {
                    tracing::debug!(%block,
                        "FakeP2PClient::state_diff_stream triggering recoverable error at",
                    );
                    // This will cause commitment mismatch
                    b.state_update
                        .as_mut()
                        .unwrap()
                        .contract_updates
                        .insert(Faker.fake(), Faker.fake());
                }

                Ok(PeerData::for_tests((
                    b.state_update.unwrap().into(),
                    b.header.header.number,
                )))
            }))
        }
    }

    impl ClassStream for FakeP2PClient {
        fn class_stream(
            self,
            start: BlockNumber,
            stop: BlockNumber,
            _: impl Stream<Item = anyhow::Result<usize>> + Send + 'static,
        ) -> impl Stream<Item = StreamItem<ClassDefinition>> + Send {
            let error_trigger = self.error_trigger.clone();

            stream::iter(
                self.blocks(start, stop, false, |mut b| {
                    let block = b.header.header.number;

                    if error_trigger.recoverable(block, 3) {
                        tracing::debug!(%block,
                            "FakeP2PClient::class_stream triggering recoverable error at",
                        );
                        // This will trigger unexpected class
                        b.cairo_defs.push((Faker.fake(), Faker.fake()));
                    }

                    let block_number = b.header.header.number;
                    b.cairo_defs
                        .into_iter()
                        .map(move |(hash, definition)| {
                            Ok(PeerData::for_tests(ClassDefinition::Cairo {
                                block_number,
                                definition,
                                hash,
                            }))
                        })
                        .chain(b.sierra_defs.into_iter().map(
                            move |(hash, sierra_definition, _, _)| {
                                Ok(PeerData::for_tests(ClassDefinition::Sierra {
                                    block_number,
                                    sierra_definition,
                                    hash,
                                }))
                            },
                        ))
                })
                .into_iter()
                .flatten(),
            )
        }
    }

    impl EventStream for FakeP2PClient {
        fn event_stream(
            self,
            start: BlockNumber,
            stop: BlockNumber,
            _: impl Stream<Item = anyhow::Result<usize>> + Send + 'static,
        ) -> impl Stream<Item = StreamItem<EventsForBlockByTransaction>> {
            let error_trigger = self.error_trigger.clone();

            stream::iter(self.blocks(start, stop, false, |mut b| {
                let block = b.header.header.number;

                if error_trigger.recoverable(block, 4) {
                    tracing::debug!(%block,
                        "FakeP2PClient::event_stream triggering recoverable error at",
                    );
                    // This will trigger event commitment mismatch
                    b.transaction_data.last_mut().unwrap().2.push(Faker.fake());
                }

                Ok(PeerData::for_tests((
                    b.header.header.number,
                    b.transaction_data
                        .into_iter()
                        .map(|(t, _, e)| (t.hash, e))
                        .collect(),
                )))
            }))
        }
    }

    impl BlockClient for FakeP2PClient {
        async fn transactions_for_block(
            self,
            block: BlockNumber,
        ) -> Option<(
            PeerId,
            impl Stream<Item = anyhow::Result<(Transaction, P2PReceipt)>> + Send,
        )> {
            let mut tr = self
                .blocks
                .iter()
                .find(|b| b.header.header.number == block)
                .unwrap()
                .transaction_data
                .iter()
                .map(|(t, r, e)| Ok((t.clone(), P2PReceipt::from(r.clone()))))
                .collect::<Vec<anyhow::Result<(Transaction, P2PReceipt)>>>();

            if self.error_trigger.recoverable(block, 6) {
                tracing::debug!(%block,
                    "FakeP2PClient::transactions_for_block triggering recoverable error at",
                );
                // This will cause transaction hash mismatch
                tr.last_mut().unwrap().as_mut().unwrap().0.variant = Faker.fake();
            }

            if self.error_trigger.fatal(block) {
                tracing::debug!(%block,
                    "FakeP2PClient::transactions_for_block triggering fatal error at",
                );
                // Returning an error from the "for_block" apis does not trigger a fatal error
                // so instead we insert a fake header for this very block to trigger an
                // insertion conflict when track is about to store the entire block
                let mut db = self.storage.connection().unwrap();
                let db = db.transaction().unwrap();
                let header = BlockHeader {
                    number: block,
                    ..Default::default()
                };
                db.insert_block_header(&header).unwrap();
                db.commit().unwrap();
            }

            Some((PeerId::random(), stream::iter(tr)))
        }

        async fn state_diff_for_block(
            self,
            block: BlockNumber,
            state_diff_length: u64,
        ) -> Result<Option<(PeerId, StateUpdateData)>, StateDiffsError> {
            let mut sd: StateUpdateData = self
                .blocks
                .iter()
                .find(|b| b.header.header.number == block)
                .unwrap()
                .state_update
                .clone()
                .unwrap()
                .into();

            assert_eq!(sd.state_diff_length(), state_diff_length);

            if self.error_trigger.recoverable(block, 7) {
                tracing::debug!(%block,
                    "FakeP2PClient::state_diff_for_block triggering recoverable error at",
                );
                // This will cause commitment mismatch
                sd.contract_updates.insert(Faker.fake(), Faker.fake());
            }

            Ok(Some((PeerId::random(), sd)))
        }

        async fn class_definitions_for_block(
            self,
            block: BlockNumber,
            declared_classes_count: u64,
        ) -> Result<Option<(PeerId, Vec<ClassDefinition>)>, ClassDefinitionsError> {
            let b = self
                .blocks
                .iter()
                .find(|b| b.header.header.number == block)
                .unwrap();
            let mut defs = b
                .cairo_defs
                .iter()
                .map(|(h, x)| ClassDefinition::Cairo {
                    block_number: block,
                    definition: x.clone(),
                    hash: *h,
                })
                .chain(
                    b.sierra_defs
                        .iter()
                        .map(|(h, x, _, _)| ClassDefinition::Sierra {
                            block_number: block,
                            sierra_definition: x.clone(),
                            hash: *h,
                        }),
                )
                .collect::<Vec<ClassDefinition>>();

            if self.error_trigger.recoverable(block, 8) {
                tracing::debug!(%block,
                    "FakeP2PClient::class_definitions_for_block triggering recoverable error at",
                );
                // This will cause unexpected class
                defs.push(Faker.fake());
            }

            Ok(Some((PeerId::random(), defs)))
        }

        async fn events_for_block(
            self,
            block: BlockNumber,
        ) -> Option<(
            PeerId,
            impl Stream<Item = Result<(TransactionHash, Event), EventsResponseStreamFailure>> + Send,
        )> {
            let mut e = self
                .blocks
                .iter()
                .find(|b| b.header.header.number == block)
                .unwrap()
                .transaction_data
                .iter()
                .flat_map(|(t, _, e)| e.iter().map(move |e| (t.hash, e.clone())))
                .map(Ok)
                .collect::<Vec<_>>();

            if self.error_trigger.recoverable(block, 9) {
                tracing::debug!(%block,
                    "FakeP2PClient::events_for_block triggering recoverable error at",
                );
                // This will trigger commitment mismatch
                e.push(Ok(Faker.fake()));
            }

            if block == LAST_IN_TRACK {
                let last_event_tx = self.last_event_tx.clone();
                last_event_tx.send(()).await.unwrap();
            }

            Some((PeerId::random(), stream::iter(e)))
        }
    }

    #[derive(Clone)]
    struct FakeFgw {
        head: (BlockNumber, BlockHash),
    }

    #[async_trait::async_trait]
    impl GatewayApi for FakeFgw {
        async fn casm_by_hash(
            &self,
            _: ClassHash,
            _: BlockId,
        ) -> Result<SerializedCasmDefinition, SequencerError> {
            Ok(SerializedCasmDefinition::from_slice(
                starknet_gateway_test_fixtures::class_definitions::CAIRO_1_1_0_BALANCE_CASM_JSON,
            ))
        }

        async fn block_header(
            &self,
            block: BlockId,
        ) -> Result<(BlockNumber, BlockHash), SequencerError> {
            assert_eq!(block, BlockId::Latest);
            Ok(self.head)
        }
    }
}
