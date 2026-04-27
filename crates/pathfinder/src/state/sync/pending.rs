use anyhow::Context;
use pathfinder_common::{BlockHash, BlockNumber};
use starknet_gateway_client::GatewayApi;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::state::sync::SyncEvent;

/// Emits new pending data events while the current block is close to the latest
/// block.
#[allow(clippy::too_many_arguments)]
pub async fn poll_pre_confirmed<S: GatewayApi + Clone + Send + 'static>(
    tx_event: tokio::sync::mpsc::Sender<SyncEvent>,
    sequencer: S,
    poll_interval: std::time::Duration,
    latest: watch::Receiver<(BlockNumber, BlockHash)>,
    current: watch::Receiver<(BlockNumber, BlockHash)>,
) {
    const IN_SYNC_THRESHOLD: u64 = 6;

    #[derive(Debug, Default)]
    struct State {
        block_number: BlockNumber,
        tx_count: usize,
        pre_latest_data_present: bool,
    }

    impl State {
        /// Returns `true` if the state was updated, `false` otherwise.
        fn update(&mut self, new_state: Self) -> bool {
            use std::cmp::Ordering;

            let should_update = match new_state.block_number.get().cmp(&self.block_number.get()) {
                Ordering::Less => false,   // Stale pre-confirmed data (older block).
                Ordering::Greater => true, // New pre-confirmed block.
                Ordering::Equal => match new_state.tx_count.cmp(&self.tx_count) {
                    Ordering::Less => false,   // Stale pre-confirmed data (fewer txs).
                    Ordering::Greater => true, // New transactions available.
                    Ordering::Equal => {
                        // Check if pre-latest data got cleared (because it has been finalized),
                        // which is a valid update if both block number and transaction count are
                        // same.
                        self.pre_latest_data_present && !new_state.pre_latest_data_present
                    }
                },
            };

            if should_update {
                *self = new_state;
            }
            should_update
        }
    }

    let mut state = State::default();

    loop {
        let t_fetch = Instant::now();

        let (latest_number, latest_hash) = *latest.borrow();
        let current_number = current.borrow().0.get();

        if latest_number.get().abs_diff(current_number) > IN_SYNC_THRESHOLD {
            tracing::debug!(
                latest = %latest_number.get(), current = %current_number,
                "Not in sync yet; skipping pre-confirmed block download"
            );
            tokio::time::sleep_until(t_fetch + poll_interval).await;
            continue;
        }

        let pre_latest_data = match fetch_pre_latest(&sequencer, latest_number, latest_hash).await {
            Ok(r) => r.map(Box::new),
            Err(e) => {
                tracing::debug!(%e, "Failed to fetch pre-latest block");
                tokio::time::sleep_until(t_fetch + poll_interval).await;
                continue;
            }
        };

        // If the pre-latest block (latest + 1) exists then the sequencer has already
        // started building the next pre-confirmed block (latest + 2).
        let pre_confirmed_block_number = if pre_latest_data.is_some() {
            latest_number + 2
        } else {
            latest_number + 1
        };
        let pre_confirmed_block = match sequencer
            .preconfirmed_block(pre_confirmed_block_number.into())
            .await
        {
            Ok(r) => r,
            Err(err) => {
                tracing::debug!(%err, "Failed to fetch pre-confirmed block");
                tokio::time::sleep_until(t_fetch + poll_interval).await;
                continue;
            }
        };

        let new_state = State {
            block_number: pre_confirmed_block_number,
            tx_count: pre_confirmed_block.transactions.len(),
            pre_latest_data_present: pre_latest_data.is_some(),
        };
        if state.update(new_state) {
            tracing::trace!("Emitting a pre-confirmed update");
            if let Err(e) = tx_event
                .send(SyncEvent::PreConfirmed {
                    number: pre_confirmed_block_number,
                    block: pre_confirmed_block.into(),
                    pre_latest_data,
                })
                .await
            {
                tracing::error!(error=%e, "Event channel closed unexpectedly. Ending pre-confirmed stream.");
                break;
            }
        } else {
            tracing::trace!("No change in pre-confirmed block data");
        }

        tokio::time::sleep_until(t_fetch + poll_interval).await;
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
            round: None,
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
            .returning(move |_| Ok(PRE_CONFIRMED_BLOCK.clone()));

        let latest_hash = PRE_LATEST_BLOCK.parent_hash;
        let latest_block_number = BlockNumber::new_or_panic(1);
        let (_, latest) = watch::channel((latest_block_number, latest_hash));
        let (_, current) = watch::channel((latest_block_number, latest_hash));

        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn(async move {
            poll_pre_confirmed(tx, sequencer, std::time::Duration::ZERO, latest, current).await
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
        sequencer.expect_preconfirmed_block().returning(move |_| {
            let mut count = COUNT.lock().unwrap();
            *count += 1;

            let block = match *count {
                1 => b0_copy.clone(),
                2 => PRE_CONFIRMED_BLOCK.clone(),
                _ => b1_copy.clone(),
            };

            Ok(block)
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
        sequencer.expect_preconfirmed_block().returning(move |_| {
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

            Ok(block)
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

    /// The expected sequence for Starknet v0.14.0+ goes something like this:
    ///   1) Node is on block N
    ///   2) Sequencer is producing pre-confirmed block N + 1 (N is still
    ///      pre-latest)
    ///   3) Block N + 1 is promoted to pre-latest and block N + 2 is being
    ///      built as pre-confirmed
    ///   4) Block N + 1 is finalized and published as the new L2 block
    ///   5) Node stores (and is now on) block N + 1
    ///
    /// Since we determine the next expected pre-confirmed block as:
    ///
    ///  if node_latest_hash == pre_latest.parent_hash {
    ///      pre_confirmed_num = node_latest_num + 2
    ///  } else {
    ///      pre_confirmed_num = node_latest_num + 1
    ///  }
    ///
    /// we must make sure that inconsistencies in the gateway responses do not
    /// cause the polling task to emit inconsistent updates.
    ///
    /// See also <https://github.com/equilibriumco/pathfinder/issues/3081>.
    #[tokio::test]
    async fn ignores_inconsistent_pre_latest_from_gateway() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut sequencer = MockGatewayApi::new();

        let our_latest_hash = PRE_LATEST_BLOCK.parent_hash;
        let mut fake_pre_latest = PRE_LATEST_BLOCK.clone();
        fake_pre_latest.parent_hash = block_hash!("0xbad");

        static COUNT: std::sync::Mutex<usize> = std::sync::Mutex::new(0);

        sequencer.expect_pending_block().returning(move || {
            let mut count = COUNT.lock().unwrap();
            let block = match *count {
                0 => {
                    *count += 1;
                    // Polling task has default state at the start, so this should produce an
                    // event. It should also set the current expected pre-confirmed block number
                    // to `latest + 2`.
                    (PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())
                }
                1 => {
                    *count += 1;
                    // This will cause a mismatch between our latest block hash and the pre-latest
                    // block parent hash. When these don't match, the expected pre-confirmed block
                    // number is `latest + 1`, which is lower than before. This should be ignored.
                    (fake_pre_latest.clone(), PENDING_UPDATE.clone())
                }
                _ => {
                    // Our latest hash and the pre-latest block parent hash match again. Since, we
                    // always send the same pre-confirmed block by the mock sequencer, this should
                    // not produce any events.
                    (PRE_LATEST_BLOCK.clone(), PENDING_UPDATE.clone())
                }
            };

            Ok(block)
        });
        sequencer
            .expect_preconfirmed_block()
            .returning(move |_| Ok(PRE_CONFIRMED_BLOCK.clone()));

        let latest_block_number = BlockNumber::new_or_panic(10);

        let (_, rx_latest) = watch::channel((latest_block_number, our_latest_hash));
        let (_, rx_current) = watch::channel((latest_block_number, our_latest_hash));

        let sequencer = Arc::new(sequencer);
        let _jh = tokio::spawn(async move {
            super::poll_pre_confirmed(
                tx,
                sequencer,
                std::time::Duration::ZERO,
                rx_latest,
                rx_current,
            )
            .await
        });

        let _ = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("First event should be emitted");
        let result = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await;
        assert!(result.is_err(), "No event should be emitted for stale data");
    }
}
