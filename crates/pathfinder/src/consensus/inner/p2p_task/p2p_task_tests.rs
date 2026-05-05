//! End-to-end tests for p2p_task
//!
//! These tests verify the full integration flow of p2p_task,
//! including proposal processing, deferral logic and rollback
//! scenarios. They test the complete path from receiving P2P events
//! to sending consensus commands.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use p2p::consensus::{peer_score, Client, Event, EventKind, HeightAndRound};
use p2p::libp2p::identity::Keypair;
use p2p::libp2p::PeerId;
use p2p_proto::consensus::ProposalPart;
use pathfinder_common::prelude::*;
use pathfinder_common::{
    consensus_info,
    ChainId,
    ConsensusFinalizedBlockHeader,
    ConsensusFinalizedL2Block,
    ContractAddress,
    ProposalCommitment,
};
use pathfinder_consensus::ConsensusCommand;
use pathfinder_crypto::Felt;
use pathfinder_storage::{Storage, StorageBuilder};
use pathfinder_validator::ValidatorWorkerPool;
use tokio::sync::{mpsc, watch};
use tokio::time::error::Elapsed;
use tokio::time::timeout;

use crate::consensus::inner::dummy_proposal::{
    create_test_proposal_init,
    create_transaction_batch,
};
use crate::consensus::inner::{
    p2p_task,
    ConsensusTaskEvent,
    ConsensusValue,
    P2PTaskConfig,
    P2PTaskEvent,
};
use crate::SyncMessageToConsensus;

/// Helper struct to setup and manage the test environment (databases,
/// channels, mock client)
struct TestEnvironment {
    main_storage: Storage,
    p2p_client_receiver: mpsc::UnboundedReceiver<p2p::core::Command<p2p::consensus::Command>>,
    p2p_tx: mpsc::UnboundedSender<Event>,
    tx_to_p2p: mpsc::Sender<P2PTaskEvent>,
    rx_from_p2p: mpsc::Receiver<ConsensusTaskEvent>,
    tx_sync_to_consensus: mpsc::Sender<SyncMessageToConsensus>,
    handle: Arc<Mutex<Option<tokio::task::JoinHandle<anyhow::Result<()>>>>>,
    // Optional so we can `take()` it in `Drop`.
    worker_pool: Option<ValidatorWorkerPool>,

    // Keep these alive to prevent receiver from being dropped
    _info_watch_rx: watch::Receiver<consensus_info::ConsensusInfo>,
}

impl TestEnvironment {
    const HISTORY_DEPTH: u64 = 10;
    const TX_TO_CONSENSUS_CHANNEL_SIZE: usize = 100;
    const TX_TO_P2P_CHANNEL_SIZE: usize = 100;

    fn new(chain_id: ChainId, validator_address: ContractAddress) -> Self {
        Self::with_finalized_blocks(chain_id, validator_address, HashMap::new())
    }

    fn with_finalized_blocks(
        chain_id: ChainId,
        validator_address: ContractAddress,
        finalized_blocks: HashMap<HeightAndRound, ConsensusFinalizedL2Block>,
    ) -> Self {
        // Initialize temp pathfinder and consensus databases
        let main_storage = StorageBuilder::in_tempdir().expect("Failed to create temp database");

        // Mock channels for p2p communication
        let (p2p_tx, p2p_rx) = mpsc::unbounded_channel();
        let (tx_to_consensus, rx_from_p2p) = mpsc::channel(Self::TX_TO_CONSENSUS_CHANNEL_SIZE);
        let (tx_to_p2p, rx_from_consensus) = mpsc::channel(Self::TX_TO_P2P_CHANNEL_SIZE);
        let (tx_sync_to_consensus, rx_from_sync) = mpsc::channel(1);
        let (info_watch_tx, info_watch_rx) =
            watch::channel(consensus_info::ConsensusInfo::default());

        // Create mock Client (used for receiving events in these tests)
        let keypair = Keypair::generate_ed25519();
        let (client_sender, client_receiver) = mpsc::unbounded_channel();
        let peer_id = keypair.public().to_peer_id();
        let p2p_client = Client::from((peer_id, client_sender));

        let (handle, worker_pool) = p2p_task::spawn(
            chain_id,
            P2PTaskConfig {
                my_starknet_version: StarknetVersion::V_0_14_0,
                my_validator_address: validator_address,
                history_depth: Self::HISTORY_DEPTH,
            },
            p2p_client,
            p2p_rx,
            tx_to_consensus,
            rx_from_consensus,
            rx_from_sync,
            info_watch_tx,
            main_storage.clone(),
            finalized_blocks,
            // Only used for failure injection, which does not happen in these tests
            &PathBuf::default(),
            pathfinder_compiler::ResourceLimits::for_test(),
            pathfinder_compiler::BlockifierLibfuncs::default(),
            true,
            None,
            None,
        );

        Self {
            main_storage,
            p2p_client_receiver: client_receiver,
            p2p_tx,
            tx_to_p2p,
            rx_from_p2p,
            tx_sync_to_consensus,
            handle: Arc::new(Mutex::new(Some(handle))),
            worker_pool: Some(worker_pool),
            _info_watch_rx: info_watch_rx,
        }
    }

    fn create_committed_block(&self, height: u64) {
        let block_id_felt = Felt::from(height);
        let mut db_conn = self.main_storage.connection().unwrap();
        let db_tx = db_conn.transaction().unwrap();

        let header = BlockHeader::builder()
            .number(BlockNumber::new_or_panic(height))
            .timestamp(BlockTimestamp::new_or_panic(1000))
            .calculated_state_commitment(
                StorageCommitment(block_id_felt),
                ClassCommitment(block_id_felt),
            )
            .sequencer_address(SequencerAddress::ZERO)
            .finalize_with_hash(BlockHash(block_id_felt));

        db_tx.insert_block_header(&header).unwrap();
        db_tx.commit().unwrap();
    }

    async fn wait_for_task_initialization(&self) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    async fn verify_task_alive(&self) {
        let handle_opt = {
            let handle_guard = self.handle.lock().unwrap();
            handle_guard.as_ref().map(|h| h.is_finished())
        };

        if let Some(true) = handle_opt {
            // Handle is finished, take it out and await to get the error
            let handle = {
                let mut handle_guard = self.handle.lock().unwrap();
                handle_guard.take().expect("Handle should exist")
            };

            match handle.await {
                Ok(Ok(())) => {
                    panic!("Task finished successfully (unexpected - should still be running)");
                }
                Ok(Err(e)) => {
                    panic!("Task finished with error: {e:#}");
                }
                Err(e) => {
                    panic!("Task panicked: {e:?}");
                }
            }
        }
    }

    async fn _wait_for_task_exit(&self) -> Result<anyhow::Result<()>, Elapsed> {
        let wait_for_exit_fut = async {
            loop {
                let handle_opt = {
                    let handle_guard = self.handle.lock().unwrap();
                    handle_guard.as_ref().map(|h| h.is_finished())
                };

                if let Some(true) = handle_opt {
                    // Handle is finished, take it out and await to get the result
                    let handle = {
                        let mut handle_guard = self.handle.lock().unwrap();
                        handle_guard.take().expect("Handle should exist")
                    };

                    return handle.await?;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        timeout(Duration::from_millis(300), wait_for_exit_fut).await
    }

    async fn wait_tx_to_p2p_consumed(&self) {
        let start = std::time::Instant::now();
        let timeout_duration = Duration::from_millis(300);

        while start.elapsed() < timeout_duration {
            if self.tx_to_p2p.capacity() == Self::TX_TO_P2P_CHANNEL_SIZE {
                // All messages consumed
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("Timeout waiting for tx_to_p2p to be consumed");
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        // Ensure the task is stopped when the test environment is dropped
        let handle_opt = {
            let mut handle_guard = self.handle.lock().unwrap();
            handle_guard.take()
        };

        if let Some(handle) = handle_opt {
            handle.abort();
        }

        // Join the worker pool to ensure all threads are cleaned up properly
        if let Some(pool) = self.worker_pool.take().and_then(Arc::into_inner) {
            pool.join();
        }
    }
}

fn create_finalized_block(height: u64) -> ConsensusFinalizedL2Block {
    ConsensusFinalizedL2Block {
        header: ConsensusFinalizedBlockHeader {
            number: BlockNumber::new_or_panic(height),
            timestamp: BlockTimestamp::new_or_panic(height + 1000),
            state_diff_commitment: StateDiffCommitment(Felt::from(height)),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Helper: Wait for a proposal event from consensus
async fn wait_for_proposal_event(
    rx: &mut mpsc::Receiver<ConsensusTaskEvent>,
    timeout_duration: Duration,
) -> Option<ConsensusCommand<ConsensusValue, ContractAddress>> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout_duration {
        // First try non-blocking recv
        match rx.try_recv() {
            Ok(ConsensusTaskEvent::CommandFromP2P(ConsensusCommand::Proposal(proposal))) => {
                return Some(ConsensusCommand::Proposal(proposal))
            }
            Ok(_) => {
                // Other event, continue waiting
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // No event yet, wait a bit
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed
                return None;
            }
        }
    }
    None
}

/// Helper: Wait for a [ConsensusCommand::ChangePeerScore] event from
/// consensus.
async fn wait_for_change_peer_score(
    p2p_client_rx: &mut mpsc::UnboundedReceiver<p2p::core::Command<p2p::consensus::Command>>,
    timeout_duration: Duration,
) -> Option<(PeerId, f64)> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout_duration {
        // First try non-blocking recv
        match p2p_client_rx.try_recv() {
            Ok(p2p::core::Command::Application(p2p::consensus::Command::ChangePeerScore {
                peer_id,
                delta,
            })) => {
                return Some((peer_id, delta));
            }
            Ok(_) => {
                // Other event, continue waiting
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // No event yet, wait a bit
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed
                eprintln!("channel closed");
                return None;
            }
        }
    }
    None
}

/// Helper: Verify no proposal event was received
async fn verify_no_proposal_event(rx: &mut mpsc::Receiver<ConsensusTaskEvent>, duration: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < duration {
        match rx.try_recv() {
            Ok(ConsensusTaskEvent::CommandFromP2P(proposal @ ConsensusCommand::Proposal(_))) => {
                panic!("Unexpected proposal event received: {proposal:?}");
            }
            Ok(_) => {
                // Other event, continue checking
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // No event, wait a bit
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed, that's fine
                return;
            }
        }
    }
}

/// Helper: Verify proposal event matches expected values
fn verify_proposal_event(
    proposal_cmd: ConsensusCommand<ConsensusValue, ContractAddress>,
    expected_height: u64,
    expected_commitment: ProposalCommitment,
) {
    match proposal_cmd {
        ConsensusCommand::Proposal(signed_proposal) => {
            assert_eq!(
                signed_proposal.proposal.height, expected_height,
                "Proposal height should match"
            );
            assert_eq!(
                signed_proposal.proposal.value.0, expected_commitment,
                "Proposal commitment should match"
            );
        }
        _ => panic!("Expected Proposal command"),
    }
}

/// ProposalFin deferred until parent block is decided.
///
/// **Scenario**: ProposalFin arrives before the parent block is decided.
/// Execution has started (TransactionBatch received), so ProposalFin must
/// be deferred until the parent block is decided (or committed, covered by
/// [test_proposal_fin_deferred_until_parent_block_committed]), then
/// finalization can proceed.
///
/// **Test**: Send Init → TransactionBatch → ProposalFin →
/// MarkBlockAsDecidedAndCleanUp(parent).
///
/// Verify ProposalFin is deferred (no proposal event), then verify
/// finalization occurs after parent block is decided.
#[rstest::rstest]
#[case::consensus_ahead_of_fgw(true)]
#[case::fgw_ahead_of_consensus(false)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_proposal_fin_deferred_until_parent_block_decided(
    #[case] consensus_ahead_of_fgw: bool,
) {
    use pathfinder_common::proposal_commitment;

    let chain_id = ChainId::SEPOLIA_TESTNET;

    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());

    let h0r0 = HeightAndRound::new(0, 0);
    let h1r0 = HeightAndRound::new(1, 0);
    let finalized_blocks = HashMap::from([
        (h0r0, create_finalized_block(h0r0.height())),
        (h1r0, create_finalized_block(h1r0.height())),
    ]);

    let proposal_commitment0 = finalized_blocks
        .get(&h0r0)
        .map(|block| block.header.state_diff_commitment.0)
        .map(ProposalCommitment)
        .unwrap();

    let expected_proposal_commitment2 =
        proposal_commitment!("0x02A3CE358B96A4A26AC9C0EF4F7A8F878A9F3B1A4757E716874CAC711617CA87");

    let mut env =
        TestEnvironment::with_finalized_blocks(chain_id, validator_address, finalized_blocks);

    env.tx_to_p2p
        .send(P2PTaskEvent::MarkBlockAsDecidedAndCleanUp(
            h0r0,
            ConsensusValue(proposal_commitment0),
        ))
        .await
        .expect("Failed to send MarkBlockAsDecidedAndCleanUp");

    if !consensus_ahead_of_fgw {
        // Simulate the case where the feeder gateway was faster to get the
        // block for H=1 from the proposer than the node under test figured
        // out that the very block was decided upon.
        env.create_committed_block(1);
    }
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let h2r1 = HeightAndRound::new(2, 1);
    let transactions = create_transaction_batch(0, 0, 5, chain_id);
    let proposal_init =
        create_test_proposal_init(chain_id, h2r1.height(), h2r1.round(), proposer_address);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(h2r1, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Step 2: Send TransactionBatch (execution should start)
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(h2r1, ProposalPart::TransactionBatch(transactions)),
        })
        .expect("Failed to send TransactionBatch");
    env.verify_task_alive().await;

    // Verify: No proposal event yet (execution started, but not finalized)
    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;

    // Step 3: Send ProposalFin
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                h2r1,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(expected_proposal_commitment2.0),
                    executed_transaction_count: 5,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    env.verify_task_alive().await;

    if consensus_ahead_of_fgw {
        // Verify: Still no proposal event
        verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;
    } else {
        // Verify: Proposal event should be sent now
        let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(3))
            .await
            .expect("Expected proposal event after ProposalFin");
        verify_proposal_event(proposal_cmd, 2, expected_proposal_commitment2);
    }

    // Step 6: Send MarkBlockAsDecidedAndCleanUp for parent block (should trigger
    // finalization)
    env.tx_to_p2p
        .send(P2PTaskEvent::MarkBlockAsDecidedAndCleanUp(
            h2r1,
            ConsensusValue(expected_proposal_commitment2),
        ))
        .await
        .expect("Failed to send MarkBlockAsDecidedAndCleanUp");
    env.verify_task_alive().await;

    // Make sure the above message is consumed before proceeding, otherwise we can
    // get an ugly race condition which does not occur in reality but will make the
    // test fail once in a while
    env.wait_tx_to_p2p_consumed().await;

    if consensus_ahead_of_fgw {
        // Step 8: At some point sync sends SyncMessageToConsensus::GetFinalizedBlock
        // for H=1, and then confirms committing the block with
        // SyncMessageToConsensus::ConfirmFinalizedBlockCommitted
        env.create_committed_block(1);
        env.tx_sync_to_consensus
            .send(SyncMessageToConsensus::ConfirmBlockCommitted {
                number: BlockNumber::new_or_panic(1),
            })
            .await
            .expect("Failed to send ConfirmBlockCommitted");
        env.verify_task_alive().await;

        // Verify: Proposal event should be sent now
        let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(3))
            .await
            .expect("Expected proposal event after ConfirmBlockCommitted");
        verify_proposal_event(proposal_cmd, 2, expected_proposal_commitment2);
    } else {
        // Step 8: It turns out that the feeder gateway was faster to get the
        // block for H=1 from the proposer than the node under test figured out
        // that the very block was decided upon.
    }
    env.verify_task_alive().await;
}

/// ProposalFin deferred until parent block is committed.
///
/// **Scenario**: ProposalFin arrives before the parent block is committed.
/// Execution has started (TransactionBatch received), so ProposalFin must be
/// deferred until the parent block is committed, then finalization can proceed.
///
/// **Test**: Send Init → TransactionBatch → ProposalFin →
/// CommitBlock(parent).
///
/// Verify ProposalFin is deferred (no proposal event), then verify
/// finalization occurs after parent block is committed.
#[rstest::rstest]
#[case::consensus_ahead_of_fgw(true)]
#[case::fgw_ahead_of_consensus(false)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_proposal_fin_deferred_until_parent_block_committed(
    #[case] consensus_ahead_of_fgw: bool,
) {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());

    let h1r0 = HeightAndRound::new(1, 0);
    let finalized_blocks = HashMap::from([(h1r0, create_finalized_block(h1r0.height()))]);

    let proposal_commitment1 = finalized_blocks
        .get(&h1r0)
        .map(|block| block.header.state_diff_commitment.0)
        .map(ProposalCommitment)
        .unwrap();

    let mut env =
        TestEnvironment::with_finalized_blocks(chain_id, validator_address, finalized_blocks);
    env.create_committed_block(0);
    if !consensus_ahead_of_fgw {
        // Simulate the case where the feeder gateway was faster to get the
        // block for H=1 from the proposer than the node under test figured
        // out that the very block was decided upon.
        env.create_committed_block(1);
    }
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let h2r1 = HeightAndRound::new(2, 1);
    let transactions = create_transaction_batch(0, 0, 5, chain_id);
    let proposal_init =
        create_test_proposal_init(chain_id, h2r1.height(), h2r1.round(), proposer_address);

    // Focus is on batch execution and deferral logic, not commitment validation.
    // Using a dummy commitment...
    let proposal_commitment2 = ProposalCommitment(Felt::ZERO);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(h2r1, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Step 2: Send TransactionBatch (execution should start)
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(h2r1, ProposalPart::TransactionBatch(transactions)),
        })
        .expect("Failed to send TransactionBatch");
    env.verify_task_alive().await;

    // Verify: No proposal event yet (execution started, but not finalized)
    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;

    // Step 3: Send ProposalFin
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                h2r1,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment2.0),
                    executed_transaction_count: 5,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    env.verify_task_alive().await;

    if consensus_ahead_of_fgw {
        // Verify: Still no proposal event
        verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;
    } else {
        // Verify: Proposal event should be sent now
        let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(3))
            .await
            .expect("Expected proposal event after ProposalFin");
        verify_proposal_event(proposal_cmd, 2, proposal_commitment2);
    }

    // Step 6: Send MarkBlockAsDecidedAndCleanUp for parent block (should trigger
    // finalization)
    env.tx_to_p2p
        .send(
            crate::consensus::inner::P2PTaskEvent::MarkBlockAsDecidedAndCleanUp(
                h1r0,
                ConsensusValue(proposal_commitment1),
            ),
        )
        .await
        .expect("Failed to send MarkBlockAsDecidedAndCleanUp");
    env.verify_task_alive().await;

    // Make sure the above message is consumed before proceeding, otherwise we can
    // get an ugly race condition which does not occur in reality but will make the
    // test fail once in a while
    env.wait_tx_to_p2p_consumed().await;

    if consensus_ahead_of_fgw {
        // Step 8: At some point sync sends SyncMessageToConsensus::GetFinalizedBlock
        // for H=1, and then confirms committing the block with
        // SyncMessageToConsensus::ConfirmBlockCommitted
        env.create_committed_block(1);
        env.tx_sync_to_consensus
            .send(SyncMessageToConsensus::ConfirmBlockCommitted {
                number: BlockNumber::new_or_panic(1),
            })
            .await
            .expect("Failed to send ConfirmBlockCommitted");
        env.verify_task_alive().await;

        // Verify: Proposal event should be sent now
        let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(3))
            .await
            .expect("Expected proposal event after ConfirmBlockCommitted");
        verify_proposal_event(proposal_cmd, 2, proposal_commitment2);
    } else {
        // Step 8: It turns out that the feeder gateway was faster to get the
        // block for H=1 from the proposer than the node under test figured out
        // that the very block was decided upon.
    }
    env.verify_task_alive().await;
}

/// Full proposal flow in normal order.
///
/// **Scenario**: Complete proposal flow with all parts arriving in the
/// expected order, no deferral is needed.
///
/// **Test**: Send Init → TransactionBatch → ProposalFin.
///
/// Verify proposal event is sent immediately after ProposalFin (no
/// deferral).
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_full_proposal_flow_normal_order() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    env.create_committed_block(1);
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);
    let transactions = create_transaction_batch(0, 0, 5, chain_id);
    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    // Focus is on batch execution and deferral logic, not commitment validation.
    // Using a dummy commitment...
    let proposal_commitment = ProposalCommitment(Felt::ZERO);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Step 2: Send TransactionBatch
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions),
            ),
        })
        .expect("Failed to send TransactionBatch");
    env.verify_task_alive().await;

    // Verify: No proposal event yet (execution started, but
    // ProposalFin not processed)
    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;

    // Step 3: Send ProposalFin
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment.0),
                    executed_transaction_count: 5,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify: Proposal event should be sent immediately (both conditions met)
    let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(2))
        .await
        .expect("Expected proposal event after ProposalFin");
    verify_proposal_event(proposal_cmd, 2, proposal_commitment);
    env.verify_task_alive().await;
}

/// TransactionBatch deferred when execution not started.
///
/// **Scenario**: Parent block is not committed initially, so
/// TransactionBatch is deferred. After parent is committed, execution
/// starts and deferred messages are processed.
///
/// **Test**: Send Init → TransactionBatch (without committing parent).
///
/// Verify no execution occurs. Then commit parent block and send another
/// TransactionBatch. Verify execution starts.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_executed_transaction_count_deferred_when_execution_not_started() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    // Parent block NOT committed initially
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);
    let transactions_batch1 = create_transaction_batch(0, 0, 3, chain_id);
    let transactions_batch2 = create_transaction_batch(0, 3, 2, chain_id); // Total: 5
    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Step 2: Send first TransactionBatch (should be deferred - parent not
    // committed)
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch1),
            ),
        })
        .expect("Failed to send first TransactionBatch");
    env.verify_task_alive().await;

    // Verify: No proposal event (execution deferred)
    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;

    // Step 3: Now we commit the parent block
    env.create_committed_block(1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Step 4: Send another TransactionBatch
    // This should trigger execution of deferred batches
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch2),
            ),
        })
        .expect("Failed to send second TransactionBatch");
    env.verify_task_alive().await;

    // At this point, execution should have started.  To verify this,
    // we send ProposalFin, then verify that a proposal event is sent
    // (which confirms executed transaction count was processed).

    // Once again, using a dummy commitment...
    let proposal_commitment = ProposalCommitment(Felt::ZERO);

    // Step 5: Send ProposalFin
    // This should trigger finalization
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment.0),
                    executed_transaction_count: 5,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    env.verify_task_alive().await;

    // Verify: Proposal event should be sent
    let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(2))
        .await
        .expect("Expected proposal event after deferred execution");
    verify_proposal_event(proposal_cmd, 2, proposal_commitment);
    env.verify_task_alive().await;
}

/// Multiple TransactionBatch messages are executed correctly.
///
/// **Scenario**: A proposal contains multiple TransactionBatch messages
/// that must all be executed in order. All batches should be executed
/// before executed transaction count is processed.
///
/// **Test**: Send Init → TransactionBatch 1 → TransactionBatch 2 →
/// TransactionBatch 3 → ProposalFin.
///
/// Verify proposal event is sent after ProposalFin.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_multiple_batches_execution() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    env.create_committed_block(1);
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);
    let transactions_batch1 = create_transaction_batch(0, 0, 2, chain_id);
    let transactions_batch2 = create_transaction_batch(0, 2, 3, chain_id);
    let transactions_batch3 = create_transaction_batch(0, 5, 2, chain_id); // Total: 7
    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    // Focus is on batch execution and deferral logic, not commitment validation.
    // Using a dummy commitment...
    let proposal_commitment = ProposalCommitment(Felt::ZERO);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Step 2: Send multiple TransactionBatches
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch1),
            ),
        })
        .expect("Failed to send TransactionBatch1");
    env.verify_task_alive().await;

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch2),
            ),
        })
        .expect("Failed to send TransactionBatch2");
    env.verify_task_alive().await;

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch3),
            ),
        })
        .expect("Failed to send TransactionBatch3");
    env.verify_task_alive().await;

    // Step 3: Send ProposalFin
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment.0),
                    executed_transaction_count: 7,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify: Proposal event should be sent
    let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(2))
        .await
        .expect("Expected proposal event after ProposalFin");
    verify_proposal_event(proposal_cmd, 2, proposal_commitment);
    env.verify_task_alive().await;
}

/// Executed transaction count triggers rollback when count is less than
/// executed.
///
/// **Scenario**: We execute 10 transactions (2 batches of 5), but
/// executed transaction count indicates only 7 transactions were
/// executed by the proposer. The validator must rollback from 10 to 7
/// transactions to match the proposer's state.
///
/// **Test**: Send Init → TransactionBatch1 (5 txs) →
/// TransactionBatch2 (5 txs) → ProposalFin (executed transaction
/// count=7).
///
/// Verify proposal event is sent successfully after rollback, confirming
/// the rollback mechanism works correctly.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_executed_transaction_count_rollback() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    env.create_committed_block(1);
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);
    let transactions_batch1 = create_transaction_batch(0, 0, 5, chain_id);
    let transactions_batch2 = create_transaction_batch(0, 5, 5, chain_id); // Total: 10
    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    // Focus is on batch execution and deferral logic, not commitment validation.
    // Using a dummy commitment...
    let proposal_commitment = ProposalCommitment(Felt::ZERO);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Step 2: Send TransactionBatch 1 (5 transactions)
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch1),
            ),
        })
        .expect("Failed to send TransactionBatch1");
    env.verify_task_alive().await;

    // Step 3: Send TransactionBatch 2 (5 more transactions, total = 10)
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions_batch2),
            ),
        })
        .expect("Failed to send TransactionBatch2");
    env.verify_task_alive().await;

    // Step 4: Send ProposalFin
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment.0),
                    executed_transaction_count: 7, // should trigger rollback from 10 to 7
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify: Proposal event should be sent (rollback completed successfully)
    //
    // NOTE: We verify that a proposal event is sent, which indicates rollback
    // completed. However, we cannot directly verify the transaction count in e2e
    // tests because the validator is internal to p2p_task. The rollback logic
    // itself is verified in unit tests
    // (batch_execution.rs::test_executed_transaction_count_rollback).
    // This e2e test verifies that rollback doesn't break the proposal flow
    // end-to-end.
    let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(2))
        .await
        .expect("Expected proposal event after ProposalFin");
    verify_proposal_event(proposal_cmd, 2, proposal_commitment);
    env.verify_task_alive().await;
}

/// Empty TransactionBatch execution (non-spec edge case).
///
/// **Scenario**: A proposal contains an empty TransactionBatch. Per
/// the [Starknet consensus
/// spec](https://raw.githubusercontent.com/starknet-io/starknet-p2p-specs/refs/heads/main/p2p/proto/consensus/consensus.md),
/// if a proposer has no transactions, they should send an empty
/// proposal (skipping TransactionBatch entirely). However, this test
/// covers the case where a non-empty proposal includes an empty
/// TransactionBatch. Such a proposal is invalid per the spec, so we
/// reject it.
///
/// **Test**: Send Init → TransactionBatch (empty) → ProposalFin
/// (executed transaction count=0).
///
/// Verify that the proposal is rejected and no proposal event is sent.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_empty_batch_is_rejected() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    env.create_committed_block(1);
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);
    let empty_transactions = create_transaction_batch(0, 0, 0, chain_id);
    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(empty_transactions),
            ),
        })
        .expect("Failed to send empty TransactionBatch");

    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;
    // Empty batch is a recoverable error, so the task should remain alive.
    env.verify_task_alive().await;
}

/// Executed transaction count indicates more transactions than actually
/// executed.
///
/// **Scenario**: We execute 5 transactions, but executed transaction
/// count indicates 10. This shouldn't happen with proper message
/// ordering, but the code handles it by logging a warning and
/// continuing.
///
/// **Test**: Send Init → TransactionBatch (5 txs) → ProposalFin
/// (executed transaction count=10).
///
/// Verify processing continues and proposal event is sent (with 5 transactions,
/// not 10).
///
/// **Note**: We cannot directly verify these things. The goal of this
/// e2e test is to verify that processing continues correctly despite the
/// mismatch.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_executed_transaction_count_exceeds_actually_executed() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    env.create_committed_block(1);
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);
    let transactions = create_transaction_batch(0, 0, 5, chain_id);
    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    let proposal_commitment = ProposalCommitment(Felt::ZERO);

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::TransactionBatch(transactions),
            ),
        })
        .expect("Failed to send TransactionBatch");
    env.verify_task_alive().await;

    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;

    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment.0),
                    executed_transaction_count: 10,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(2))
        .await
        .expect("Expected proposal event after ProposalFin");
    verify_proposal_event(proposal_cmd, 2, proposal_commitment);
    env.verify_task_alive().await;
}

/// Empty proposal per spec (no TransactionBatch).
///
/// **Scenario**: A proposer cannot offer a valid proposal, so the height is
/// agreed to be empty. Per the spec, empty proposals skip TransactionBatch
/// entirely. The order is:
/// ProposalInit → ProposalFin.
///
/// **Test**: Send ProposalInit → ProposalFin (no TransactionBatch).
///
/// Verify ProposalFin proceeds immediately (not deferred, since execution
/// never started), proposal event is sent.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_empty_proposal_per_spec() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    env.create_committed_block(1);
    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());
    let height_and_round = HeightAndRound::new(2, 1);

    let proposal_init = create_test_proposal_init(chain_id, 2, 1, proposer_address);

    // Using a dummy commitment...
    let proposal_commitment = ProposalCommitment(Felt::ZERO);

    // Step 1: Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // Verify: No proposal event yet (ProposalFin not received)
    verify_no_proposal_event(&mut env.rx_from_p2p, Duration::from_millis(200)).await;

    // Step 2: Send ProposalFin
    // Since execution never started (no TransactionBatch), ProposalFin should
    // proceed immediately without deferral.
    env.p2p_tx
        .send(Event {
            source: PeerId::random(),
            kind: EventKind::Proposal(
                height_and_round,
                ProposalPart::Fin(p2p_proto::consensus::ProposalFin {
                    proposal_commitment: p2p_proto::common::Hash(proposal_commitment.0),
                    executed_transaction_count: 0,
                    fin_payload: None,
                }),
            ),
        })
        .expect("Failed to send ProposalFin");
    env.verify_task_alive().await;

    // Verify: Proposal event should be sent immediately (not deferred)
    // This confirms that ProposalFin proceeds when execution never started,
    // which is the correct behavior for empty proposals per spec.
    let proposal_cmd = wait_for_proposal_event(&mut env.rx_from_p2p, Duration::from_secs(2))
        .await
        .expect("Expected proposal event after ProposalFin for empty proposal");
    verify_proposal_event(proposal_cmd, 2, proposal_commitment);
    env.verify_task_alive().await;
}

/// Make sure that receiving an outdated P2P message results in a command
/// that punishes the peer that sent the outdated message.
#[tokio::test(flavor = "multi_thread")]
async fn recv_outdated_event_changes_peer_score() {
    let chain_id = ChainId::SEPOLIA_TESTNET;
    let validator_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x123").unwrap());
    let mut env = TestEnvironment::new(chain_id, validator_address);
    // Latest height (the only in this case) must be higher than the proposal height
    // + history.
    env.create_committed_block(TestEnvironment::HISTORY_DEPTH + 4);
    let proposal_height_and_round = HeightAndRound::new(2, 1);

    env.wait_for_task_initialization().await;

    let proposer_address = ContractAddress::new_or_panic(Felt::from_hex_str("0x456").unwrap());

    // We'll use an empty proposal, the content isn't important.
    let proposal_init = create_test_proposal_init(
        chain_id,
        proposal_height_and_round.height(),
        proposal_height_and_round.round(),
        proposer_address,
    );

    let outdated_event_source = PeerId::random();

    // Send ProposalInit
    env.p2p_tx
        .send(Event {
            source: outdated_event_source,
            kind: EventKind::Proposal(proposal_height_and_round, ProposalPart::Init(proposal_init)),
        })
        .expect("Failed to send ProposalInit");
    env.verify_task_alive().await;

    // As soon as we receive an outdated command, the P2P client should receive the
    // command to penalize the peer.
    let (peer_id, delta) =
        wait_for_change_peer_score(&mut env.p2p_client_receiver, Duration::from_secs(2))
            .await
            .expect("Expected change peer score command after outdated ProposalInit");

    assert_eq!(peer_id, outdated_event_source);
    assert_eq!(delta, peer_score::penalty::OUTDATED_MESSAGE);
}
