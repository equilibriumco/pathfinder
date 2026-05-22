use std::sync::Arc;

use anyhow::Context;
use pathfinder_common::{BlockHash, BlockNumber};
use pathfinder_pre_confirmed::PendingDataCache;
use starknet_gateway_client::{BlockId, GatewayApi};
use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::state::sync::SyncEvent;

/// Maximum gap, in blocks, between `current` and `latest` for pre-confirmed
/// polling. Beyond this we skip as the node is still catching up.
const IN_SYNC_THRESHOLD: u64 = 6;

#[derive(Debug)]
struct State {
    /// Height we're currently polling.
    block_number: BlockNumber,
    /// Server-given block identifier from the last successful poll. `None`
    /// until we've completed our first poll for this height. The server uses
    /// this to detect when our view is stale and needs a full rebuild.
    block_identifier: Option<String>,
    /// Running merged view of the preconfirmed block at `block_number`.
    /// `None` until we've received our first response for this height.
    accumulated: Option<PreConfirmedBlock>,
    /// Whether the pre-latest block was present at the last poll.
    pre_latest_data_present: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            block_number: BlockNumber::GENESIS,
            block_identifier: None,
            accumulated: None,
            pre_latest_data_present: false,
        }
    }
}

impl State {
    fn tx_count(&self) -> u64 {
        self.accumulated
            .as_ref()
            .map(|b| b.transactions.len() as u64)
            .unwrap_or(0)
    }

    /// Apply a fresh poll response, given the pre-latest presence
    /// observed for this poll. Returns `true` if `accumulated` was
    /// updated and the caller should emit the new view.
    fn apply(&mut self, response: PreConfirmedPollResponse, new_pre_latest: bool) -> bool {
        let prev_pre_latest = self.pre_latest_data_present;
        self.pre_latest_data_present = new_pre_latest;

        match response {
            PreConfirmedPollResponse::Unchanged => false,

            PreConfirmedPollResponse::Delta {
                identifier,
                new_transactions,
                new_receipts,
                new_state_diffs,
            } => {
                // Per spec, the server only sends a delta when its identifier matches
                // ours. A mismatch indicates a server bug or local state corruption;
                // skip defensively and wait for the next poll (which our stored identifier
                // will not match server's, triggering a full rebuild).
                if self.block_identifier.as_ref() != Some(&identifier) {
                    tracing::warn!(
                        ours = ?self.block_identifier,
                        theirs = %identifier,
                        "delta response identifier doesn't match ours; skipping"
                    );
                    return false;
                }
                if new_transactions.is_empty() {
                    return false;
                }
                let acc = self
                    .accumulated
                    .as_mut()
                    .expect("accumulated present whenever block_identifier is Some");
                acc.transactions.extend(new_transactions);
                acc.transaction_receipts.extend(new_receipts);
                acc.transaction_state_diffs.extend(new_state_diffs);
                true
            }

            PreConfirmedPollResponse::Full {
                identifier,
                block_number: _,
                block,
            } => {
                // Emit on any of three independent signals:
                //  - the server's identifier changed (round bump, new height, or first poll)
                //  - new transactions arrived
                //  - pre-latest just finalised
                // Otherwise suppress: pre-0.14.3 gateways re-serve the same view across
                // polls and we don't want to bombard downstream with redundant events.
                let identifier_changed = self.block_identifier.as_ref() != Some(&identifier);
                let new_txs_arrived = (block.transactions.len() as u64) > self.tx_count();
                let pre_latest_finalised = prev_pre_latest && !new_pre_latest;

                if identifier_changed || new_txs_arrived || pre_latest_finalised {
                    self.block_identifier = Some(identifier);
                    self.accumulated = Some(block);
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// Emits new pending data events while the current block is close to the latest
/// block.
///
/// Suspends polling after `inactivity_timeout` elapses without the cache
/// being read, and resumes on the next read.
pub(super) async fn poll_pre_confirmed<S: GatewayApi + Clone + Send + 'static>(
    tx_event: tokio::sync::mpsc::Sender<SyncEvent>,
    sequencer: S,
    poll_interval: std::time::Duration,
    cache: Arc<PendingDataCache>,
    inactivity_timeout: std::time::Duration,
    latest: watch::Receiver<(BlockNumber, BlockHash)>,
    current: watch::Receiver<(BlockNumber, BlockHash)>,
) {
    let mut state = State::default();
    let mut last_active = Instant::now();

    loop {
        // Suspend if idle.
        if last_active.elapsed() >= inactivity_timeout && cache.subscriber_count() == 0 {
            tracing::debug!("Pre-confirmed polling idle; waiting for cache reads");
            cache.mark_idle();
            cache.wait_for_read().await;
            last_active = Instant::now();
        }

        let t_fetch = Instant::now();

        // Skip while catching up to head.
        let (latest_number, latest_hash) = *latest.borrow();
        let current_number = current.borrow().0.get();

        if latest_number.get().abs_diff(current_number) > IN_SYNC_THRESHOLD {
            tracing::debug!(
                latest = %latest_number.get(), current = %current_number,
                "Not in sync yet; skipping pre-confirmed block download"
            );
            if wait_for_next_poll(t_fetch + poll_interval, &cache).await {
                last_active = Instant::now();
            }
            continue;
        }

        // Fetch pre-latest and pre-confirmed.
        let pre_latest_data = match fetch_pre_latest(&sequencer, latest_number, latest_hash).await {
            Ok(r) => r.map(Box::new),
            Err(e) => {
                tracing::debug!(%e, "Failed to fetch pre-latest block");
                if wait_for_next_poll(t_fetch + poll_interval, &cache).await {
                    last_active = Instant::now();
                }
                continue;
            }
        };

        let response = match sequencer
            .preconfirmed_block(
                BlockId::Latest,
                state.block_identifier.clone(),
                state.tx_count(),
            )
            .await
        {
            Ok(r) => r,
            Err(err) => {
                tracing::debug!(%err, "Failed to fetch pre-confirmed block");
                if wait_for_next_poll(t_fetch + poll_interval, &cache).await {
                    last_active = Instant::now();
                }
                continue;
            }
        };

        // Reconcile state with the resolved height. Only `Full` carries a
        // height; `Unchanged`/`Delta` leave state at its tracked block.
        let resolved = match response {
            PreConfirmedPollResponse::Full { block_number, .. } => Some(block_number),
            _ => None,
        };

        if let Some(height) = resolved {
            // A transient (?) lower-height shouldn't discard accumulated state.
            if height < state.block_number && state.block_number != BlockNumber::GENESIS {
                tracing::debug!(
                    resolved = %height,
                    current = %state.block_number,
                    "Pre-confirmed resolved to a lower height than tracked; skipping poll"
                );
                if wait_for_next_poll(t_fetch + poll_interval, &cache).await {
                    last_active = Instant::now();
                }
                continue;
            }

            // The chain advanced: complete the previous block before resetting state.
            if height > state.block_number {
                if state.block_identifier.is_some() && state.accumulated.is_some() {
                    complete_previous_block(&sequencer, &mut state, &tx_event).await;
                }
                state = State {
                    block_number: height,
                    ..State::default()
                };
            }
        }

        // Publish. Each branch is responsible for signalling cache freshness:
        //   - changed: the emitted event leads to a `cache.store(...)` call, which
        //     bumps freshness as a side effect.
        //   - unchanged: nothing is emitted, so we call `cache.refresh()` here to
        //     unblock cold-start readers with the existing cache contents.
        if state.apply(response, pre_latest_data.is_some()) {
            let accumulated = state
                .accumulated
                .as_ref()
                .expect("accumulated block present after a successful update");
            tracing::trace!("Emitting a pre-confirmed update");
            if let Err(e) = tx_event
                .send(SyncEvent::PreConfirmed {
                    number: state.block_number,
                    block: accumulated.clone().into(),
                    pre_latest_data,
                })
                .await
            {
                tracing::error!(error=%e, "Event channel closed unexpectedly. Ending pre-confirmed stream.");
                break;
            }
        } else {
            tracing::trace!("No change in pre-confirmed block data");
            cache.refresh();
        }

        // Wait for the next tick.
        if wait_for_next_poll(t_fetch + poll_interval, &cache).await {
            last_active = Instant::now();
        }
    }
}

/// Sleeps until `deadline`, returning early if the cache is read. Returns
/// `true` when woken by a read, `false` when the deadline elapsed.
async fn wait_for_next_poll(deadline: Instant, cache: &PendingDataCache) -> bool {
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => false,
        _ = cache.wait_for_read() => true,
    }
}

/// Fetch the previously-tracked block one final time to capture any
/// transactions that landed before it was superseded. Best-effort.
async fn complete_previous_block<S: GatewayApi + Send + 'static>(
    sequencer: &S,
    state: &mut State,
    tx_event: &tokio::sync::mpsc::Sender<SyncEvent>,
) {
    let response = match sequencer
        .preconfirmed_block(
            state.block_number.into(),
            state.block_identifier.clone(),
            state.tx_count(),
        )
        .await
    {
        Ok(r) => r,
        Err(err) => {
            tracing::debug!(%err, "Completion query for previous pre-confirmed block failed");
            return;
        }
    };

    if state.apply(response, false) {
        let accumulated = state
            .accumulated
            .as_ref()
            .expect("accumulated present after successful completion apply");
        if let Err(e) = tx_event
            .send(SyncEvent::PreConfirmed {
                number: state.block_number,
                block: accumulated.clone().into(),
                pre_latest_data: None,
            })
            .await
        {
            tracing::error!(error=%e, "Event channel closed during completion emission");
        }
    }
}

/// Fetch the pending block from the sequencer and classify it as
/// [pre-latest](starknet_gateway_types::reply::PreLatestBlock) if its parent
/// hash matches our latest block hash.
///
/// If the pre-latest block (N) exists, the sequencer has already started
/// building the next pre-confirmed block (N + 1).
async fn fetch_pre_latest<S: GatewayApi + Send + 'static>(
    sequencer: &S,
    our_latest_number: BlockNumber,
    our_latest_hash: BlockHash,
) -> anyhow::Result<
    Option<(
        BlockNumber,
        starknet_gateway_types::reply::PreLatestBlock,
        pathfinder_common::StateUpdate,
    )>,
> {
    let (pending_block, state_update) = sequencer
        .pending_block()
        .await
        .context("Fetching pre-latest block from sequencer")?;

    let pre_latest_data = (pending_block.parent_hash == our_latest_hash).then_some((
        our_latest_number + 1,
        pending_block,
        state_update,
    ));
    Ok(pre_latest_data)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, LazyLock};

    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_common::transaction::{
        DataAvailabilityMode,
        InvokeTransactionV3,
        L1HandlerTransaction,
        Transaction,
        TransactionVariant,
    };
    use pathfinder_pre_confirmed::PendingDataCache;
    use starknet_gateway_client::MockGatewayApi;
    use starknet_gateway_types::reply::state_update::{
        DeclaredSierraClass,
        DeployedContract,
        MigratedCompiledClass,
        ReplacedClass,
        StateDiff,
        StorageDiff,
    };
    use starknet_gateway_types::reply::{
        Block,
        GasPrices,
        L1DataAvailabilityMode,
        PreConfirmedBlock,
        PreConfirmedPollResponse,
        PreLatestBlock,
        Status,
    };
    use tokio::sync::watch;

    use super::poll_pre_confirmed;
    use crate::state::sync::SyncEvent;

    const PARENT_HASH: BlockHash = block_hash!("0x1234");
    const PARENT_ROOT: StateCommitment = state_commitment_bytes!(b"parent root");

    pub static NEXT_BLOCK: LazyLock<Block> = LazyLock::new(|| Block {
        block_hash: block_hash!("0xabcd"),
        block_number: BlockNumber::new_or_panic(1),
        l1_gas_price: Default::default(),
        l1_data_gas_price: Default::default(),
        l2_gas_price: Default::default(),
        parent_block_hash: PARENT_HASH,
        sequencer_address: None,
        state_commitment: PARENT_ROOT,
        status: Status::AcceptedOnL2,
        timestamp: BlockTimestamp::new_or_panic(10),
        transaction_receipts: Vec::new(),
        transactions: Vec::new(),
        starknet_version: StarknetVersion::default(),
        l1_da_mode: Default::default(),
        transaction_commitment: Default::default(),
        event_commitment: Default::default(),
        receipt_commitment: Default::default(),
        state_diff_commitment: Default::default(),
        state_diff_length: Default::default(),
    });

    pub static PENDING_UPDATE: LazyLock<StateUpdate> =
        LazyLock::new(|| StateUpdate::default().with_parent_state_commitment(PARENT_ROOT));

    pub static PRE_LATEST_BLOCK: LazyLock<PreLatestBlock> = LazyLock::new(|| PreLatestBlock {
        l1_gas_price: GasPrices {
            price_in_wei: GasPrice(11),
            ..Default::default()
        },
        l1_data_gas_price: Default::default(),
        l2_gas_price: Default::default(),
        parent_hash: NEXT_BLOCK.parent_block_hash,
        sequencer_address: sequencer_address_bytes!(b"seqeunecer address"),
        status: Status::Pending,
        timestamp: BlockTimestamp::new_or_panic(20),
        transaction_receipts: Vec::new(),
        transactions: vec![pathfinder_common::transaction::Transaction {
            hash: transaction_hash!("0x22"),
            variant: pathfinder_common::transaction::TransactionVariant::L1Handler(
                L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x2"),
                    calldata: Vec::new(),
                },
            ),
        }],
        starknet_version: StarknetVersion::new(0, 14, 0, 0),
        l1_da_mode: L1DataAvailabilityMode::Calldata,
    });

    pub static PRE_CONFIRMED_BLOCK: LazyLock<PreConfirmedBlock> =
        LazyLock::new(|| PreConfirmedBlock {
            l1_gas_price: Default::default(),
            l1_data_gas_price: Default::default(),
            l2_gas_price: Default::default(),
            sequencer_address: sequencer_address_bytes!(b"seqeunecer address"),
            status: Status::PreConfirmed,
            timestamp: BlockTimestamp::new_or_panic(30),
            starknet_version: StarknetVersion::new(0, 14, 0, 0),
            l1_da_mode: L1DataAvailabilityMode::Blob,
            transactions: vec![
                pathfinder_common::transaction::Transaction {
                    hash: transaction_hash!("0x22"),
                    variant: pathfinder_common::transaction::TransactionVariant::L1Handler(
                        L1HandlerTransaction {
                            contract_address: contract_address!("0x1"),
                            entry_point_selector: entry_point!("0x55"),
                            nonce: transaction_nonce!("0x2"),
                            calldata: Vec::new(),
                        },
                    ),
                },
                pathfinder_common::transaction::Transaction {
                    hash: transaction_hash!("0x33"),
                    variant: pathfinder_common::transaction::TransactionVariant::InvokeV3(
                        InvokeTransactionV3 {
                            signature: vec![],
                            nonce: transaction_nonce!("0x3"),
                            nonce_data_availability_mode: DataAvailabilityMode::L1,
                            fee_data_availability_mode: DataAvailabilityMode::L1,
                            resource_bounds: Default::default(),
                            tip: Default::default(),
                            paymaster_data: vec![],
                            account_deployment_data: vec![],
                            calldata: vec![],
                            sender_address: contract_address!("0x2"),
                            proof_facts: vec![],
                        },
                    ),
                },
            ],
            transaction_receipts: vec![
                Some((
                    pathfinder_common::receipt::Receipt {
                        actual_fee: Default::default(),
                        execution_resources: Default::default(),
                        l2_to_l1_messages: vec![],
                        execution_status: pathfinder_common::receipt::ExecutionStatus::Succeeded,
                        transaction_hash: transaction_hash!("0x22"),
                        transaction_index: TransactionIndex::new_or_panic(0),
                    },
                    vec![],
                )),
                None,
            ],
            transaction_state_diffs: vec![
                Some(StateDiff {
                    storage_diffs: HashMap::from([(
                        contract_address_bytes!(b"contract 0"),
                        vec![StorageDiff {
                            key: storage_address_bytes!(b"storage key 0"),
                            value: storage_value_bytes!(b"storage val 0"),
                        }],
                    )]),
                    deployed_contracts: vec![DeployedContract {
                        address: contract_address_bytes!(b"deployed contract"),
                        class_hash: class_hash_bytes!(b"deployed class"),
                    }],
                    old_declared_contracts: HashSet::from([
                        class_hash_bytes!(b"cairo 0 0"),
                        class_hash_bytes!(b"cairo 0 1"),
                    ]),
                    declared_classes: vec![DeclaredSierraClass {
                        class_hash: sierra_hash_bytes!(b"sierra class"),
                        compiled_class_hash: casm_hash_bytes!(b"casm hash"),
                    }],
                    nonces: HashMap::from([
                        (
                            contract_address_bytes!(b"contract 0"),
                            contract_nonce_bytes!(b"nonce 0"),
                        ),
                        (
                            contract_address_bytes!(b"contract 10"),
                            contract_nonce_bytes!(b"nonce 10"),
                        ),
                    ]),
                    replaced_classes: vec![ReplacedClass {
                        address: contract_address_bytes!(b"contract 0"),
                        class_hash: class_hash_bytes!(b"replaced class"),
                    }],
                    migrated_compiled_classes: vec![MigratedCompiledClass {
                        class_hash: sierra_hash_bytes!(b"migrated class"),
                        compiled_class_hash: casm_hash_bytes!(b"migrated casm"),
                    }],
                }),
                None,
            ],
        });

    /// Arbitrary timeout for receiving emits on the tokio channel. Otherwise
    /// failing tests will need to timeout naturally which may be forever.
    const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    #[tokio::test]
    async fn success() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut sequencer = MockGatewayApi::new();

        sequencer
            .expect_pending_block()
            .returning(|| Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                Ok(PreConfirmedPollResponse::Full {
                    identifier: String::new(),
                    block_number: BlockNumber::new_or_panic(3),
                    block: PRE_CONFIRMED_BLOCK.clone(),
                })
            });

        let latest_hash = PRE_LATEST_BLOCK.parent_hash;
        let latest_block_number = BlockNumber::new_or_panic(1);
        let (_, latest) = watch::channel((latest_block_number, latest_hash));
        let (_, current) = watch::channel((latest_block_number, latest_hash));

        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn(async move {
            poll_pre_confirmed(
                tx,
                sequencer,
                std::time::Duration::ZERO,
                Arc::new(PendingDataCache::new()),
                std::time::Duration::from_secs(60),
                latest,
                current,
            )
            .await
        });

        let result = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("Event should be emitted")
            .unwrap();

        let expected_pre_latest_data = Some(Box::new((
            latest_block_number + 1,
            PRE_LATEST_BLOCK.clone(),
            PENDING_UPDATE.clone(),
        )));

        assert_matches!(
            result,
            SyncEvent::PreConfirmed {
                number,
                block,
                pre_latest_data,
            } if number == latest_block_number + 2
                && *block == *PRE_CONFIRMED_BLOCK
                && pre_latest_data == expected_pre_latest_data
        );
    }

    #[tokio::test]
    async fn ignores_inconsistent_gateway_blocks() {
        // In this test the gateway mock sends inconsistent block data.
        //
        // It first sends a block with 1 tx, then 0 and then 2.
        // We expect the function to ignore the middle one since pending data
        // should be monotonically growing.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut sequencer = MockGatewayApi::new();

        let mut b0 = PRE_CONFIRMED_BLOCK.clone();
        b0.transactions.push(Transaction {
            hash: transaction_hash!("0x22"),
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!("0x1"),
                entry_point_selector: entry_point!("0x55"),
                nonce: transaction_nonce!("0x2"),
                calldata: Vec::new(),
            }),
        });
        let b0_copy = b0.clone();

        let mut b1 = b0.clone();
        b1.transactions.push(Transaction {
            hash: transaction_hash!("0x22"),
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!("0x1"),
                entry_point_selector: entry_point!("0x55"),
                nonce: transaction_nonce!("0x2"),
                calldata: Vec::new(),
            }),
        });
        let b1_copy = b1.clone();

        static COUNT: std::sync::Mutex<usize> = std::sync::Mutex::new(0);

        sequencer
            .expect_pending_block()
            .returning(move || Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                let mut count = COUNT.lock().unwrap();
                *count += 1;

                let block = match *count {
                    1 => b0_copy.clone(),
                    2 => PRE_CONFIRMED_BLOCK.clone(),
                    _ => b1_copy.clone(),
                };

                Ok(PreConfirmedPollResponse::Full {
                    identifier: String::new(),
                    block_number: BlockNumber::new_or_panic(3),
                    block,
                })
            });

        let sequencer = Arc::new(sequencer);
        let latest_hash = PRE_LATEST_BLOCK.parent_hash;
        let latest_block_number = BlockNumber::new_or_panic(1);
        let (_, rx_latest) = watch::channel((latest_block_number, latest_hash));
        let (_, rx_current) = watch::channel((latest_block_number, latest_hash));
        let _jh = tokio::spawn(async move {
            poll_pre_confirmed(
                tx,
                sequencer,
                std::time::Duration::ZERO,
                Arc::new(PendingDataCache::new()),
                std::time::Duration::from_secs(60),
                rx_latest,
                rx_current,
            )
            .await
        });

        let result1 = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("Event should be emitted")
            .unwrap();

        let expected_pre_latest_data = Some(Box::new((
            latest_block_number + 1,
            PRE_LATEST_BLOCK.clone(),
            PENDING_UPDATE.clone(),
        )));

        assert_matches!(
            result1,
            SyncEvent::PreConfirmed {
                number,
                block,
                pre_latest_data,
            } if number == latest_block_number + 2
                && *block == b0
                && pre_latest_data == expected_pre_latest_data
        );

        let result2 = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("Event should be emitted")
            .unwrap();

        let expected_pre_latest_data = Some(Box::new((
            latest_block_number + 1,
            PRE_LATEST_BLOCK.clone(),
            PENDING_UPDATE.clone(),
        )));

        assert_matches!(
            result2,
            SyncEvent::PreConfirmed {
                number,
                block,
                pre_latest_data,
            } if number == latest_block_number + 2
                && *block == b1
                && pre_latest_data == expected_pre_latest_data
        );
    }

    #[tokio::test]
    async fn fetch_pre_latest_returns_some_when_parent_matches() {
        let mut sequencer = MockGatewayApi::new();
        let our_latest_number = NEXT_BLOCK.block_number - 1;
        let our_latest_hash = NEXT_BLOCK.parent_block_hash;

        sequencer
            .expect_pending_block()
            .returning(move || Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));

        let (number, block, state_update) =
            super::fetch_pre_latest(&sequencer, our_latest_number, our_latest_hash)
                .await
                .unwrap()
                .unwrap();

        assert_eq!(number, NEXT_BLOCK.block_number);
        assert_eq!(block.parent_hash, our_latest_hash);
        assert_eq!(state_update, PENDING_UPDATE.clone());
    }

    #[tokio::test]
    async fn fetch_pre_latest_returns_none_when_parent_differs() {
        let mut sequencer = MockGatewayApi::new();
        let our_latest_number = NEXT_BLOCK.block_number - 1;
        let our_latest_hash = NEXT_BLOCK.parent_block_hash;
        let different_hash = block_hash!("0xdeadbeef");

        let pending_block = PreLatestBlock {
            parent_hash: different_hash,
            ..PRE_LATEST_BLOCK.clone()
        };

        sequencer
            .expect_pending_block()
            .returning(move || Ok((pending_block.clone(), PENDING_UPDATE.clone())));

        let result = super::fetch_pre_latest(&sequencer, our_latest_number, our_latest_hash)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn stale_transactions_is_ignored() {
        // This test ensures that when `poll_pre_confirmed` receives pre-confirmed
        // blocks with stale data (same or lower transaction count), no event is
        // emitted.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut sequencer = MockGatewayApi::new();

        let our_latest_hash = PRE_LATEST_BLOCK.parent_hash;

        let mut stale_pre_confirmed = PRE_CONFIRMED_BLOCK.clone();
        stale_pre_confirmed.transactions.pop();
        stale_pre_confirmed.transaction_receipts.pop();
        stale_pre_confirmed.transaction_state_diffs.pop();

        static COUNT: std::sync::Mutex<usize> = std::sync::Mutex::new(0);

        sequencer
            .expect_pending_block()
            .returning(move || Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                let mut count = COUNT.lock().unwrap();
                let block = match *count {
                    0 => {
                        *count += 1;
                        // Polling task has default state at the start, so this should produce an
                        // event.
                        PRE_CONFIRMED_BLOCK.clone()
                    }
                    1 => {
                        *count += 1;
                        // Same transaction count as before, should be ignored.
                        PRE_CONFIRMED_BLOCK.clone()
                    }
                    _ => {
                        // Lower transaction count than before, should be ignored.
                        stale_pre_confirmed.clone()
                    }
                };

                Ok(PreConfirmedPollResponse::Full {
                    identifier: String::new(),
                    block_number: BlockNumber::new_or_panic(12),
                    block,
                })
            });

        let latest_block_number = BlockNumber::new_or_panic(10);

        let (_, rx_latest) = watch::channel((latest_block_number, our_latest_hash));
        let (_, rx_current) = watch::channel((latest_block_number, our_latest_hash));

        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn(async move {
            super::poll_pre_confirmed(
                tx,
                sequencer,
                std::time::Duration::ZERO,
                Arc::new(PendingDataCache::new()),
                std::time::Duration::from_secs(60),
                rx_latest,
                rx_current,
            )
            .await
        });

        let _ = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("First event should be emitted");
        let result = tokio::time::timeout(TEST_TIMEOUT, rx.recv()).await;
        assert!(result.is_err(), "No event should be emitted for stale data");
    }

    mod apply {
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::transaction::{
            L1HandlerTransaction,
            Transaction,
            TransactionVariant,
        };
        use starknet_gateway_types::reply::{PreConfirmedBlock, PreConfirmedPollResponse};

        use super::super::{BlockNumber, State};

        fn placeholder_tx(index: usize) -> Transaction {
            let hash = match index {
                0 => transaction_hash!("0x1"),
                1 => transaction_hash!("0x2"),
                _ => transaction_hash!("0x3"),
            };
            Transaction {
                hash,
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x0"),
                    calldata: Vec::new(),
                }),
            }
        }

        /// Build a minimal `PreConfirmedBlock` with `n` placeholder
        /// transactions and matching empty receipt/state-diff slots.
        /// Other fields use `Default`.
        fn block_with_txs(n: usize) -> PreConfirmedBlock {
            let txs: Vec<Transaction> = (0..n).map(placeholder_tx).collect();
            let receipts = vec![None; n];
            let state_diffs = vec![None; n];
            PreConfirmedBlock {
                transactions: txs,
                transaction_receipts: receipts,
                transaction_state_diffs: state_diffs,
                ..Default::default()
            }
        }

        fn state(identifier: Option<&str>, txs: usize, pre_latest: bool) -> State {
            State {
                block_number: BlockNumber::new_or_panic(10),
                block_identifier: identifier.map(String::from),
                accumulated: if identifier.is_some() {
                    Some(block_with_txs(txs))
                } else {
                    None
                },
                pre_latest_data_present: pre_latest,
            }
        }

        #[test]
        fn unchanged_response_is_noop() {
            let mut s = state(Some("abc"), 1, false);
            let original_accumulated = s.accumulated.clone();
            let original_identifier = s.block_identifier.clone();
            let original_number = s.block_number;

            let emitted = s.apply(PreConfirmedPollResponse::Unchanged, true);

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, original_identifier);
            assert_eq!(s.block_number, original_number);
            // apply always writes the new pre-latest presence
            assert!(s.pre_latest_data_present);
        }

        #[test]
        fn delta_with_mismatching_identifier_is_skipped() {
            let mut s = state(Some("abc"), 1, false);
            let original_accumulated = s.accumulated.clone();

            let emitted = s.apply(
                PreConfirmedPollResponse::Delta {
                    identifier: "xyz".into(),
                    new_transactions: vec![Transaction {
                        hash: transaction_hash!("0x99"),
                        variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                            contract_address: contract_address!("0x1"),
                            entry_point_selector: entry_point!("0x55"),
                            nonce: transaction_nonce!("0x0"),
                            calldata: Vec::new(),
                        }),
                    }],
                    new_receipts: vec![None],
                    new_state_diffs: vec![None],
                },
                false,
            );

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn delta_with_matching_identifier_and_empty_transactions_is_noop() {
            let mut s = state(Some("abc"), 1, false);
            let original_accumulated = s.accumulated.clone();

            let emitted = s.apply(
                PreConfirmedPollResponse::Delta {
                    identifier: "abc".into(),
                    new_transactions: vec![],
                    new_receipts: vec![],
                    new_state_diffs: vec![],
                },
                false,
            );

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn delta_with_matching_identifier_appends() {
            let mut s = state(Some("abc"), 1, false);

            let new_tx = Transaction {
                hash: transaction_hash!("0x99"),
                variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                    contract_address: contract_address!("0x1"),
                    entry_point_selector: entry_point!("0x55"),
                    nonce: transaction_nonce!("0x0"),
                    calldata: Vec::new(),
                }),
            };

            let emitted = s.apply(
                PreConfirmedPollResponse::Delta {
                    identifier: "abc".into(),
                    new_transactions: vec![new_tx],
                    new_receipts: vec![None],
                    new_state_diffs: vec![None],
                },
                false,
            );

            assert!(emitted);
            let acc = s.accumulated.as_ref().unwrap();
            assert_eq!(acc.transactions.len(), 2);
            assert_eq!(acc.transaction_receipts.len(), 2);
            assert_eq!(acc.transaction_state_diffs.len(), 2);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }

        #[test]
        fn full_with_changed_identifier_emits() {
            let mut s = state(Some("abc"), 1, false);
            let new_block = block_with_txs(1);

            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "xyz".into(),
                    block_number: BlockNumber::new_or_panic(10),
                    block: new_block.clone(),
                },
                false,
            );

            assert!(emitted);
            assert_eq!(s.block_identifier, Some("xyz".into()));
            assert_eq!(s.accumulated, Some(new_block));
        }

        #[test]
        fn full_with_more_transactions_emits() {
            let mut s = state(Some("abc"), 1, false);
            let new_block = block_with_txs(2);

            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "abc".into(),
                    block_number: BlockNumber::new_or_panic(10),
                    block: new_block.clone(),
                },
                false,
            );

            assert!(emitted);
            assert_eq!(s.accumulated.as_ref().unwrap().transactions.len(), 2);
        }

        #[test]
        fn full_with_pre_latest_finalised_emits() {
            let mut s = state(Some("abc"), 2, true);
            let same_block = block_with_txs(2);

            // pre_latest transitions true → false: should force an emit
            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "abc".into(),
                    block_number: BlockNumber::new_or_panic(10),
                    block: same_block.clone(),
                },
                false,
            );

            assert!(emitted);
            assert_eq!(s.accumulated, Some(same_block));
        }

        #[test]
        fn full_with_no_signals_is_deduped() {
            let mut s = state(Some("abc"), 2, false);
            let same_block = block_with_txs(2);
            let original_accumulated = s.accumulated.clone();

            // Same identifier, no new txs, no pre-latest transition: nothing to emit
            let emitted = s.apply(
                PreConfirmedPollResponse::Full {
                    identifier: "abc".into(),
                    block_number: BlockNumber::new_or_panic(10),
                    block: same_block,
                },
                false,
            );

            assert!(!emitted);
            assert_eq!(s.accumulated, original_accumulated);
            assert_eq!(s.block_identifier, Some("abc".into()));
        }
    }

    /// A transient gateway inconsistency could briefly resolve `latest` to a
    /// lower height than we're already tracking. The polling loop should
    /// skip those responses without discarding accumulated state.
    ///
    /// See also <https://github.com/equilibriumco/pathfinder/issues/3081>.
    #[tokio::test]
    async fn skips_poll_when_resolved_height_is_lower() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut sequencer = MockGatewayApi::new();

        let our_latest_hash = PRE_LATEST_BLOCK.parent_hash;

        static COUNT: std::sync::Mutex<usize> = std::sync::Mutex::new(0);

        sequencer
            .expect_pending_block()
            .returning(move || Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                let mut count = COUNT.lock().unwrap();
                let block_number = match *count {
                    0 => {
                        *count += 1;
                        // First poll establishes the tracked height at 12.
                        BlockNumber::new_or_panic(12)
                    }
                    _ => {
                        // Subsequent polls resolve to a lower height (gateway hiccup).
                        BlockNumber::new_or_panic(11)
                    }
                };
                Ok(PreConfirmedPollResponse::Full {
                    identifier: String::new(),
                    block_number,
                    block: PRE_CONFIRMED_BLOCK.clone(),
                })
            });

        let latest_block_number = BlockNumber::new_or_panic(10);

        let (_, rx_latest) = watch::channel((latest_block_number, our_latest_hash));
        let (_, rx_current) = watch::channel((latest_block_number, our_latest_hash));

        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn(async move {
            super::poll_pre_confirmed(
                tx,
                sequencer,
                std::time::Duration::ZERO,
                Arc::new(PendingDataCache::new()),
                std::time::Duration::from_secs(60),
                rx_latest,
                rx_current,
            )
            .await
        });

        let first = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("First event should be emitted")
            .unwrap();
        assert_matches!(
            first,
            SyncEvent::PreConfirmed { number, .. } if number == BlockNumber::new_or_panic(12)
        );

        let result = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await;
        assert!(
            result.is_err(),
            "No event should be emitted for a backwards-resolved height"
        );
    }

    /// When the chain advances past the pre-confirmed block we're tracking,
    /// the loop completes the previous block with one final query (picking up
    /// any transactions that landed just before it was superseded) before
    /// moving on to the new height.
    ///
    /// Three emissions are expected across two ticks:
    ///   1. Block N initial
    ///   2. Block N completed (tail transaction included)
    ///   3. Block N+1 fresh
    #[tokio::test]
    async fn completes_previous_block_when_chain_advances() {
        use starknet_gateway_client::BlockId;

        let our_latest_number = BlockNumber::new_or_panic(10);
        // Pre-latest is absent throughout: our latest hash does not match
        // PRE_LATEST_BLOCK.parent_hash, so fetch_pre_latest classifies it as None.
        let our_latest_hash = block_hash!("0xabcd");

        const BLOCK_11_ID: &str = "id-11";
        const BLOCK_12_ID: &str = "id-12";

        let tail_tx = Transaction {
            hash: transaction_hash!("0x012345"),
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!("0x1"),
                entry_point_selector: entry_point!("0x55"),
                nonce: transaction_nonce!("0x2"),
                calldata: Vec::new(),
            }),
        };

        // Mock gateway
        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_pending_block()
            .returning(|| Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));

        // Pre-confirmed responses are scripted by dispatching on the request's
        // (BlockId, identifier) — each pattern fires on a specific tick.
        let tail_for_mock = tail_tx.clone();
        sequencer
            .expect_preconfirmed_block()
            .returning(move |block_id, identifier, _tx_count| {
                match (block_id, identifier.as_deref()) {
                    // Tick 1: first poll, no cached identifier. Server returns
                    // pre-confirmed block 11.
                    (BlockId::Latest, None) => Ok(PreConfirmedPollResponse::Full {
                        identifier: BLOCK_11_ID.into(),
                        block_number: BlockNumber::new_or_panic(11),
                        block: PRE_CONFIRMED_BLOCK.clone(),
                    }),

                    // Tick 2: chain has advanced. Server returns pre-confirmed
                    // block 12 with a fresh identifier.
                    (BlockId::Latest, Some(id)) if id == BLOCK_11_ID => {
                        Ok(PreConfirmedPollResponse::Full {
                            identifier: BLOCK_12_ID.into(),
                            block_number: BlockNumber::new_or_panic(12),
                            block: PRE_CONFIRMED_BLOCK.clone(),
                        })
                    }

                    // Completion query for block 11: server returns a delta
                    // carrying the tail transaction that landed just before
                    // the chain advanced.
                    (BlockId::Number(n), Some(id))
                        if n == BlockNumber::new_or_panic(11) && id == BLOCK_11_ID =>
                    {
                        Ok(PreConfirmedPollResponse::Delta {
                            identifier: BLOCK_11_ID.into(),
                            new_transactions: vec![tail_for_mock.clone()],
                            new_receipts: vec![None],
                            new_state_diffs: vec![None],
                        })
                    }

                    // Steady state on block 12: subsequent polls report no change.
                    (BlockId::Latest, Some(id)) if id == BLOCK_12_ID => {
                        Ok(PreConfirmedPollResponse::Unchanged)
                    }

                    _ => panic!(
                        "unexpected preconfirmed_block call: id={:?} identifier={:?}",
                        block_id, identifier
                    ),
                }
            });

        // Spawn the polling loop
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let (_, latest) = watch::channel((our_latest_number, our_latest_hash));
        let (_, current) = watch::channel((our_latest_number, our_latest_hash));
        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn(async move {
            super::poll_pre_confirmed(
                tx,
                sequencer,
                std::time::Duration::ZERO,
                Arc::new(PendingDataCache::new()),
                std::time::Duration::from_secs(60),
                latest,
                current,
            )
            .await
        });

        // Assertions

        // 1. Initial pre-confirmed at block 11.
        let initial = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("Initial event should be emitted")
            .unwrap();
        assert_matches!(
            initial,
            SyncEvent::PreConfirmed { number, block, pre_latest_data }
                if number == BlockNumber::new_or_panic(11)
                    && block.transactions.len() == PRE_CONFIRMED_BLOCK.transactions.len()
                    && pre_latest_data.is_none()
        );

        // 2. Block 11 completed: same height, tail transaction appended.
        let completed = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("Completion event should be emitted")
            .unwrap();
        let expected_completed_len = PRE_CONFIRMED_BLOCK.transactions.len() + 1;
        assert_matches!(
            completed,
            SyncEvent::PreConfirmed { number, block, pre_latest_data }
                if number == BlockNumber::new_or_panic(11)
                    && block.transactions.len() == expected_completed_len
                    && pre_latest_data.is_none()
        );

        // 3. Fresh pre-confirmed at block 12.
        let advanced = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("Advance event should be emitted")
            .unwrap();
        assert_matches!(
            advanced,
            SyncEvent::PreConfirmed { number, block, pre_latest_data }
                if number == BlockNumber::new_or_panic(12)
                    && block.transactions.len() == PRE_CONFIRMED_BLOCK.transactions.len()
                    && pre_latest_data.is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_pauses_polling_until_cache_read() {
        // Polling runs while the cache keeps being read; once `IDLE_TIMEOUT`
        // elapses with no reads the loop suspends. A subsequent cache read
        // wakes it back up.

        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
        const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

        // Set up: distinct identifier per call so every poll produces an emission.
        let counter = Arc::new(std::sync::Mutex::new(0u64));
        let mut sequencer = MockGatewayApi::new();
        sequencer
            .expect_pending_block()
            .returning(|| Ok((PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())));
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_, _, _| {
                let mut c = counter.lock().unwrap();
                *c += 1;
                Ok(PreConfirmedPollResponse::Full {
                    identifier: format!("id-{}", *c),
                    block_number: BlockNumber::new_or_panic(3),
                    block: PRE_CONFIRMED_BLOCK.clone(),
                })
            });

        let latest_hash = PRE_LATEST_BLOCK.parent_hash;
        let latest_block_number = BlockNumber::new_or_panic(1);
        let (_, latest) = watch::channel((latest_block_number, latest_hash));
        let (_, current) = watch::channel((latest_block_number, latest_hash));

        let cache = Arc::new(PendingDataCache::new());

        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn({
            let cache = cache.clone();
            async move {
                super::poll_pre_confirmed(
                    tx,
                    sequencer,
                    POLL_INTERVAL,
                    cache,
                    IDLE_TIMEOUT,
                    latest,
                    current,
                )
                .await
            }
        });

        // Each `recv` either returns the next emission, or times out (which
        // means the loop has suspended waiting for a cache read).
        let mut emissions_before_idle = 0;
        loop {
            match tokio::time::timeout(IDLE_TIMEOUT * 2, rx.recv()).await {
                Ok(Some(_)) => emissions_before_idle += 1,
                Ok(None) => panic!("event channel closed unexpectedly"),
                Err(_) => break,
            }
        }
        assert!(
            emissions_before_idle >= 1,
            "expected at least one emission while Active"
        );

        // A cache read wakes the loop within one poll interval.
        let _ = cache.read().await;
        tokio::time::timeout(POLL_INTERVAL * 2, rx.recv())
            .await
            .expect("loop should resume promptly after cache read")
            .expect("event channel closed");
    }
}
