//! Build pathfinder and simulated feeder gateway in debug:
//! ```
//! cargo build -p pathfinder --bin pathfinder -F p2p -F consensus-integration-tests
//! cargo build -p feeder-gateway -F small_aggregate_filters --bin feeder-gateway
//! ```
//!
//! Run the test:
//! ```
//! PATHFINDER_TEST_ENABLE_MARKER_FILES=1 cargo nextest run --test consensus -p pathfinder --features p2p,consensus-integration-tests
//! ```
//!
//! # Important
//!
//! Please do not use `cargo test` as it does not run parallel test cases in
//! separate processes, and this will interfere with `PathfinderInstance` using
//! `SIGCHLD` to detect that a Pathfinder process has exited.

#[cfg(all(feature = "p2p", feature = "consensus-integration-tests"))]
mod common;

#[cfg(all(feature = "p2p", feature = "consensus-integration-tests"))]
mod test {
    use std::time::Duration;
    use std::vec;

    use futures::StreamExt;
    use pathfinder_lib::config::integration_testing::{InjectFailureConfig, InjectFailureTrigger};
    use rstest::rstest;
    use tokio::sync::mpsc;

    use crate::common::feeder_gateway::FeederGateway;
    use crate::common::pathfinder_instance::{respawn_on_fail, PathfinderInstance};
    use crate::common::rpc_client::{
        get_cached_artifacts_info,
        get_consensus_info,
        wait_for_block_exists,
        wait_for_height,
        ApplicationPeerScore,
        ConsensusInfoOutput,
        JsonRpcReply,
    };
    use crate::common::utils;

    /// The different scenarios we want to test in the
    /// `consensus_3_nodes_with_failures` test case.
    #[derive(Copy, Clone)]
    enum ConsensusScenario {
        /// Start from clear genesis, without a bootstrapped devnet DB. All
        /// transactions get reverted since they're random, invalid L1
        /// handlers, but we get to test starting completely from
        /// scratch without relying on any pre-initialized database state.
        Genesis,
        /// Start with a bootstrapped devnet DB, all transactions are valid and
        /// should get accepted, testing the happy path with a
        /// bootstrapped DB.
        BootstrapSuccess,
        /// Start with a bootstrapped devnet DB but inject failures at different
        /// stages of the consensus flow, testing the network's ability
        /// to recover from them and make progress despite them.
        BootstrapFailure(InjectFailureConfig),
    }

    impl ConsensusScenario {
        fn is_bootstrap(&self) -> bool {
            matches!(
                self,
                ConsensusScenario::BootstrapSuccess | ConsensusScenario::BootstrapFailure(_)
            )
        }

        fn is_failure(&self) -> bool {
            matches!(self, ConsensusScenario::BootstrapFailure(_))
        }
    }

    // TODO Test cases that should be supported by the integration tests:
    // - proposals:
    //   - [x] non-empty proposals (transactions that modify storage):
    //      - Garbage L1-transaction handlers, that get reverted, but we get to test
    //        from clear genesis (no bootstrap devnet DB), exercised in the happy
    //        path scenario
    //      - Valid declare, deploy, and invoke transactions, exercised in all the
    //        other scenarios, thanks to a bootstrapped devnet DB
    //   - [ ] empty proposals, which follow the spec, ie. no transaction batches:
    //      - ProposalInit,
    //      - ProposalFin,
    // - node set sizes:
    //   - [x] 3 nodes, network stalls if 1 node fails,
    //   - [x] 4 nodes, network continues if 1 node fails, catchup via sync
    //     mechanism is activated (`fourth_node_joins_late_can_catch_up` is
    //     sufficient here),
    // - [x] failure injection (tests recovery from crashes/terminations at
    //   different stages),
    // - [ ] ??? any missing significant failure injection points ???.
    #[rstest]
    #[case::happy_path_from_genesis_reverted_txns(ConsensusScenario::Genesis)]
    #[case::happy_path_from_bootstrapped(ConsensusScenario::BootstrapSuccess)]
    #[case::fail_on_proposal_init_rx(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::proposal_init_rx(4)
    ))]
    #[case::fail_on_transaction_batch_rx(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::transaction_batch_rx(4)
    ))]
    #[case::fail_on_proposal_fin_rx(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::proposal_fin_rx(4)
    ))]
    #[case::fail_on_proposal_finalized(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::proposal_finalized(4)
    ))]
    #[case::fail_on_prevote_rx(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::prevote_rx(4)
    ))]
    #[case::fail_on_precommit_rx(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::precommit_rx(4)
    ))]
    #[case::fail_on_proposal_decided(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::proposal_decided(4)
    ))]
    #[case::fail_on_proposal_committed(ConsensusScenario::BootstrapFailure(
        InjectFailureConfig::proposal_committed(4)
    ))]
    #[tokio::test]
    async fn consensus_3_nodes_with_failures(#[case] scenario: ConsensusScenario) {
        const NUM_NODES: usize = 3;
        const READY_TIMEOUT: Duration = Duration::from_secs(20);
        const TEST_TIMEOUT: Duration = Duration::from_secs(120);
        const POLL_READY: Duration = Duration::from_millis(500);
        const POLL_HEIGHT: Duration = Duration::from_secs(1);

        let bootstrap_db = scenario.is_bootstrap();
        let disallow_reverted_txns = bootstrap_db;
        let (configs, boot_height, stopwatch) = utils::setup(NUM_NODES, bootstrap_db).unwrap();

        // System contracts start to matter after block 10 but we have a separate
        // regression test for that, which checks that rollback at H>10 works correctly.
        let target_height: u64 = boot_height + 5;

        let alice_cfg = configs.first().unwrap();
        let mut fgw = FeederGateway::spawn(alice_cfg).unwrap();
        fgw.wait_for_ready(POLL_READY, READY_TIMEOUT).await.unwrap();

        // We want everybody to have sync enabled so that not only Alice, Bob, and
        // Charlie decide upon the new blocks but also they are able to **commit the
        // blocks to their main DBs**. The trick is that MOST OF THE TIME the FGw will
        // not provide any meaningful data to the 3 nodes because it's feeding
        // off of Alice's DB which means it'll always be lagging behind the
        // nodes that achieve consensus. However in reality, the FGw, will be sometimes
        // able to provide some blocks to Bob or Charlie faster than they themselves
        // acquire a positive decision from their consensus engines.
        //
        // Additionally, dummy proposal creation relies on the parent block being
        // committed to the main DB, so sync needs to be enabled for that as well.
        let mut configs = configs.into_iter().map(|cfg| {
            cfg.with_local_feeder_gateway(fgw.port())
                .with_sync_enabled()
        });

        let alice = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();
        alice
            .wait_for_ready(POLL_READY, READY_TIMEOUT)
            .await
            .unwrap();

        let boot_port = alice.consensus_p2p_port();
        let boot_peer_id = utils::read_id_fixture("Alice").unwrap().peer_id;
        let mut configs = configs.map(|cfg| cfg.with_boot_peer(boot_port, boot_peer_id));

        let bob_cfg = {
            let bob_cfg = configs.next().unwrap();
            match scenario {
                ConsensusScenario::BootstrapFailure(inject_failure) => {
                    bob_cfg.with_inject_failure(Some(inject_failure))
                }
                _ => bob_cfg,
            }
        };

        let bob = PathfinderInstance::spawn(bob_cfg.clone()).unwrap();
        let charlie = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();

        let (bob_rdy, charlie_rdy) = tokio::join!(
            bob.wait_for_ready(POLL_READY, READY_TIMEOUT),
            charlie.wait_for_ready(POLL_READY, READY_TIMEOUT)
        );
        bob_rdy.unwrap();
        charlie_rdy.unwrap();

        utils::log_elapsed(stopwatch);

        let (hnr_tx, hnr_rx) = mpsc::channel(target_height as usize * 3);
        let hnr_rx = tokio_stream::wrappers::ReceiverStream::new(hnr_rx);
        let (err_tx, err_rx) = mpsc::channel(6);

        let alice_decided = wait_for_height(
            &alice,
            target_height,
            POLL_HEIGHT,
            Some(hnr_tx),
            err_tx.clone(),
        );
        let bob_decided = wait_for_height(&bob, target_height, POLL_HEIGHT, None, err_tx.clone());
        let charlie_decided =
            wait_for_height(&charlie, target_height, POLL_HEIGHT, None, err_tx.clone());
        let alice_committed = wait_for_block_exists(
            &alice,
            target_height,
            POLL_HEIGHT,
            disallow_reverted_txns,
            err_tx.clone(),
        );
        let bob_committed = wait_for_block_exists(
            &bob,
            target_height,
            POLL_HEIGHT,
            disallow_reverted_txns,
            err_tx.clone(),
        );
        let charlie_committed = wait_for_block_exists(
            &charlie,
            target_height,
            POLL_HEIGHT,
            disallow_reverted_txns,
            err_tx.clone(),
        );

        let maybe_bob = respawn_on_fail(
            scenario.is_failure(),
            bob,
            bob_cfg,
            POLL_READY,
            READY_TIMEOUT,
        );

        // Wait for: the test to pass, timeout, user interruption, or bail out early if
        // the RPC client encounters an error
        utils::join_all(
            vec![
                alice_decided,
                bob_decided,
                charlie_decided,
                alice_committed,
                bob_committed,
                charlie_committed,
            ],
            TEST_TIMEOUT,
            err_rx,
        )
        .await
        .unwrap();

        let decided_hnrs = hnr_rx.collect::<Vec<_>>().await;
        if let Some(x) = decided_hnrs.iter().find(|hnr| hnr.round() > 0) {
            println!("Network failed to recover in round 0 at (h:r): {x}");
        }

        let alice_artifacts = get_cached_artifacts_info(&alice, target_height)
            .await
            .unwrap();
        assert!(
            alice_artifacts.is_empty(),
            "Alice should not have leftover cached consensus data: {alice_artifacts:#?}"
        );

        if let Some(bob) = maybe_bob.instance() {
            let bob_artifacts = get_cached_artifacts_info(&bob, target_height)
                .await
                .unwrap();
            assert!(
                bob_artifacts.is_empty(),
                "Bob should not have leftover cached consensus data after respawn: \
                 {bob_artifacts:#?}"
            );
        }

        let charlie_artifacts = get_cached_artifacts_info(&charlie, target_height)
            .await
            .unwrap();
        assert!(
            charlie_artifacts.is_empty(),
            "Charlie should not have leftover cached consensus data: {charlie_artifacts:#?}"
        );
    }

    // This is not a rstest, because a) we want to keep the naming
    // convention (because the CI pipeline uses `consensus_[34]_nodes`
    // to group tests), and b) we don't normally want to run the test
    // with just 3 nodes (because that often - although not always -
    // fails).
    #[tokio::test]
    #[ignore]
    async fn consensus_3_nodes_with_lost_vote() {
        consensus_with_lost_vote(3).await;
    }

    #[tokio::test]
    async fn consensus_4_nodes_with_lost_vote() {
        consensus_with_lost_vote(4).await;
    }

    async fn consensus_with_lost_vote(num_nodes: usize) {
        const READY_TIMEOUT: Duration = Duration::from_secs(20);
        const TEST_TIMEOUT: Duration = Duration::from_secs(120);
        const POLL_READY: Duration = Duration::from_millis(500);
        const POLL_HEIGHT: Duration = Duration::from_secs(1);

        let disallow_reverted_txns = true;
        let vote_lost = Some(InjectFailureConfig {
            height: 4,
            trigger: InjectFailureTrigger::CommittedVoteLost,
        });
        let proposal_committed = Some(InjectFailureConfig {
            height: 4,
            trigger: InjectFailureTrigger::ProposalCommitted,
        });

        let (configs, boot_height, stopwatch) =
            utils::setup(num_nodes, disallow_reverted_txns).unwrap();

        let target_height: u64 = boot_height + 5;

        let alice_cfg = configs.first().unwrap();
        let mut fgw = FeederGateway::spawn(alice_cfg).unwrap();
        fgw.wait_for_ready(POLL_READY, READY_TIMEOUT).await.unwrap();

        let mut configs = configs.into_iter().map(|cfg| {
            cfg.with_local_feeder_gateway(fgw.port())
                .with_sync_enabled()
        });

        let alice_cfg = configs.next().unwrap().with_inject_failure(vote_lost);
        let alice = PathfinderInstance::spawn(alice_cfg).unwrap();
        alice
            .wait_for_ready(POLL_READY, READY_TIMEOUT)
            .await
            .unwrap();

        let boot_port = alice.consensus_p2p_port();
        let boot_peer_id = utils::read_id_fixture("Alice").unwrap().peer_id;
        let mut configs = configs.map(|cfg| cfg.with_boot_peer(boot_port, boot_peer_id));

        let bob_cfg = configs
            .next()
            .unwrap()
            .with_inject_failure(proposal_committed);
        let bob = PathfinderInstance::spawn(bob_cfg.clone()).unwrap();

        let charlie_cfg = configs.next().unwrap().with_inject_failure(vote_lost);
        let charlie = PathfinderInstance::spawn(charlie_cfg).unwrap();

        let maybe_dan = if num_nodes > 3 {
            // Dan might as well process the vote - it's not enough...
            let dan = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();
            let (bob_rdy, charlie_rdy, dan_rdy) = tokio::join!(
                bob.wait_for_ready(POLL_READY, READY_TIMEOUT),
                charlie.wait_for_ready(POLL_READY, READY_TIMEOUT),
                dan.wait_for_ready(POLL_READY, READY_TIMEOUT),
            );
            bob_rdy.unwrap();
            charlie_rdy.unwrap();
            dan_rdy.unwrap();
            Some(dan)
        } else {
            let (bob_rdy, charlie_rdy) = tokio::join!(
                bob.wait_for_ready(POLL_READY, READY_TIMEOUT),
                charlie.wait_for_ready(POLL_READY, READY_TIMEOUT),
            );
            bob_rdy.unwrap();
            charlie_rdy.unwrap();
            None
        };

        utils::log_elapsed(stopwatch);

        let (hnr_tx, hnr_rx) = mpsc::channel(target_height as usize * 3);
        let hnr_rx = tokio_stream::wrappers::ReceiverStream::new(hnr_rx);
        let (err_tx, err_rx) = mpsc::channel(6);

        let alice_decided = wait_for_height(
            &alice,
            target_height,
            POLL_HEIGHT,
            Some(hnr_tx),
            err_tx.clone(),
        );
        let bob_decided = wait_for_height(&bob, target_height, POLL_HEIGHT, None, err_tx.clone());
        let charlie_decided =
            wait_for_height(&charlie, target_height, POLL_HEIGHT, None, err_tx.clone());

        let maybe_dan_decided = if let Some(ref dan) = maybe_dan {
            let dan_decided =
                wait_for_height(dan, target_height, POLL_HEIGHT, None, err_tx.clone());
            Some(dan_decided)
        } else {
            None
        };

        let alice_committed = wait_for_block_exists(
            &alice,
            target_height,
            POLL_HEIGHT,
            disallow_reverted_txns,
            err_tx.clone(),
        );
        let bob_committed = wait_for_block_exists(
            &bob,
            target_height,
            POLL_HEIGHT,
            disallow_reverted_txns,
            err_tx.clone(),
        );
        let charlie_committed = wait_for_block_exists(
            &charlie,
            target_height,
            POLL_HEIGHT,
            disallow_reverted_txns,
            err_tx.clone(),
        );

        let maybe_dan_committed = if let Some(ref dan) = maybe_dan {
            let dan_committed = wait_for_block_exists(
                dan,
                target_height,
                POLL_HEIGHT,
                disallow_reverted_txns,
                err_tx.clone(),
            );
            Some(dan_committed)
        } else {
            None
        };

        let maybe_bob = respawn_on_fail(true, bob, bob_cfg, POLL_READY, READY_TIMEOUT);

        // Wait for: the test to pass, timeout, user interruption, or bail out early if
        // the RPC client encounters an error
        let mut all_decided = vec![alice_decided, bob_decided, charlie_decided];
        if let Some(dan_decided) = maybe_dan_decided {
            all_decided.push(dan_decided);
        }
        let mut all_committed = vec![alice_committed, bob_committed, charlie_committed];
        if let Some(dan_committed) = maybe_dan_committed {
            all_committed.push(dan_committed);
        }
        all_decided.append(&mut all_committed);
        utils::join_all(all_decided, TEST_TIMEOUT, err_rx)
            .await
            .unwrap();

        let decided_hnrs = hnr_rx.collect::<Vec<_>>().await;
        if let Some(x) = decided_hnrs.iter().find(|hnr| hnr.round() > 0) {
            println!("Network failed to recover in round 0 at (h:r): {x}");
        }

        let alice_artifacts = get_cached_artifacts_info(&alice, target_height)
            .await
            .unwrap();
        assert!(
            alice_artifacts.is_empty(),
            "Alice should not have leftover cached consensus data: {alice_artifacts:#?}"
        );

        if let Some(bob) = maybe_bob.instance() {
            let bob_artifacts = get_cached_artifacts_info(&bob, target_height)
                .await
                .unwrap();
            assert!(
                bob_artifacts.is_empty(),
                "Bob should not have leftover cached consensus data after respawn: \
                 {bob_artifacts:#?}"
            );
        }

        let charlie_artifacts = get_cached_artifacts_info(&charlie, target_height)
            .await
            .unwrap();
        assert!(
            charlie_artifacts.is_empty(),
            "Charlie should not have leftover cached consensus data: {charlie_artifacts:#?}"
        );

        if let Some(dan) = maybe_dan {
            let dan_artifacts = get_cached_artifacts_info(&dan, target_height)
                .await
                .unwrap();
            assert!(
                dan_artifacts.is_empty(),
                "Dan should not have leftover cached consensus data: {dan_artifacts:#?}"
            );
        }
    }

    #[tokio::test]
    async fn consensus_3_nodes_fourth_node_joins_late_can_catch_up() {
        const NUM_NODES: usize = 4;
        const READY_TIMEOUT: Duration = Duration::from_secs(20);
        const RUNUP_TIMEOUT: Duration = Duration::from_secs(60);
        const CATCHUP_TIMEOUT: Duration = Duration::from_secs(60);
        const POLL_READY: Duration = Duration::from_millis(500);
        const POLL_HEIGHT: Duration = Duration::from_secs(1);

        let (configs, boot_height, stopwatch) = utils::setup(NUM_NODES, true).unwrap();

        // System contracts start to matter after block 10
        let height_to_add_fourth_node: u64 = boot_height + 3;
        let target_height: u64 = height_to_add_fourth_node + 2;

        let alice_cfg = configs.first().unwrap();
        let mut fgw = FeederGateway::spawn(alice_cfg).unwrap();
        fgw.wait_for_ready(POLL_READY, READY_TIMEOUT).await.unwrap();

        // We want everybody to have sync enabled so that not only Alice, Bob, and
        // Charlie decide upon the new blocks but also they are able to **commit the
        // blocks to their main DBs**. The trick is that MOST OF THE TIME the FGw will
        // not provide any meaningful data to the 3 nodes because it's feeding
        // off of Alice's DB which means it'll always be lagging behind the
        // nodes that achieve consensus. However in reality, the FGw, will be sometimes
        // able to provide some blocks to Bob or Charlie faster than they themselves
        // acquire a positive decision from their consensus engines.
        //
        // This means that initially Dan will be actually syncing from the FGw until he
        // catches up with the other nodes, at which point he should be committing the
        // consensus-decided blocks to his own main DB, before actually sync is able to
        // get them from the FGw.
        //
        // Additionally, dummy proposal creation relies on the parent block being
        // committed to the main DB, so sync needs to be enabled for that as well.
        let mut configs = configs.into_iter().map(|cfg| {
            cfg.with_local_feeder_gateway(fgw.port())
                .with_sync_enabled()
        });
        let alice = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();
        alice
            .wait_for_ready(POLL_READY, READY_TIMEOUT)
            .await
            .unwrap();

        let boot_port = alice.consensus_p2p_port();
        let boot_peer_id = utils::read_id_fixture("Alice").unwrap().peer_id;
        let mut configs = configs.map(|cfg| cfg.with_boot_peer(boot_port, boot_peer_id));

        let bob = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();
        let charlie = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();

        let (bob_rdy, charlie_rdy) = tokio::join!(
            bob.wait_for_ready(POLL_READY, READY_TIMEOUT),
            charlie.wait_for_ready(POLL_READY, READY_TIMEOUT)
        );
        bob_rdy.unwrap();
        charlie_rdy.unwrap();

        utils::log_elapsed(stopwatch);

        let (err_tx, err_rx) = mpsc::channel(6);

        let alice_decided = wait_for_height(
            &alice,
            height_to_add_fourth_node,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );
        let bob_decided = wait_for_height(
            &bob,
            height_to_add_fourth_node,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );
        let charlie_decided = wait_for_height(
            &charlie,
            height_to_add_fourth_node,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );
        let alice_committed = wait_for_block_exists(
            &alice,
            height_to_add_fourth_node,
            POLL_HEIGHT,
            true,
            err_tx.clone(),
        );
        let bob_committed = wait_for_block_exists(
            &bob,
            height_to_add_fourth_node,
            POLL_HEIGHT,
            true,
            err_tx.clone(),
        );
        let charlie_committed = wait_for_block_exists(
            &charlie,
            height_to_add_fourth_node,
            POLL_HEIGHT,
            true,
            err_tx.clone(),
        );

        utils::join_all(
            vec![
                alice_decided,
                bob_decided,
                charlie_decided,
                alice_committed,
                bob_committed,
                charlie_committed,
            ],
            RUNUP_TIMEOUT,
            err_rx,
        )
        .await
        .unwrap();

        let dan_cfg = configs.next().unwrap().with_sync_enabled();

        let dan = PathfinderInstance::spawn(dan_cfg.clone()).unwrap();
        dan.wait_for_ready(POLL_READY, READY_TIMEOUT).await.unwrap();

        let (err_tx, err_rx) = mpsc::channel(8);

        let alice_decided =
            wait_for_height(&alice, target_height, POLL_HEIGHT, None, err_tx.clone());
        let bob_decided = wait_for_height(&bob, target_height, POLL_HEIGHT, None, err_tx.clone());
        let charlie_decided =
            wait_for_height(&charlie, target_height, POLL_HEIGHT, None, err_tx.clone());
        let dan_decided = wait_for_height(&dan, target_height, POLL_HEIGHT, None, err_tx.clone());
        let alice_committed =
            wait_for_block_exists(&alice, target_height, POLL_HEIGHT, true, err_tx.clone());
        let bob_committed =
            wait_for_block_exists(&bob, target_height, POLL_HEIGHT, true, err_tx.clone());
        let charlie_committed =
            wait_for_block_exists(&charlie, target_height, POLL_HEIGHT, true, err_tx.clone());
        let dan_committed =
            wait_for_block_exists(&dan, target_height, POLL_HEIGHT, true, err_tx.clone());

        utils::join_all(
            vec![
                alice_decided,
                bob_decided,
                charlie_decided,
                dan_decided,
                alice_committed,
                bob_committed,
                charlie_committed,
                dan_committed,
            ],
            CATCHUP_TIMEOUT,
            err_rx,
        )
        .await
        .unwrap();

        let alice_artifacts = get_cached_artifacts_info(&alice, target_height)
            .await
            .unwrap();
        assert!(
            alice_artifacts.is_empty(),
            "Alice should not have leftover cached consensus data: {alice_artifacts:#?}"
        );

        let bob_artifacts = get_cached_artifacts_info(&bob, target_height)
            .await
            .unwrap();
        assert!(
            bob_artifacts.is_empty(),
            "Bob should not have leftover cached consensus data: {bob_artifacts:#?}"
        );

        let charlie_artifacts = get_cached_artifacts_info(&charlie, target_height)
            .await
            .unwrap();
        assert!(
            charlie_artifacts.is_empty(),
            "Charlie should not have leftover cached consensus data: {charlie_artifacts:#?}"
        );

        let dan_artifacts = get_cached_artifacts_info(&dan, target_height)
            .await
            .unwrap();
        assert!(
            dan_artifacts.is_empty(),
            "Dan should not have leftover cached consensus data: {dan_artifacts:#?}"
        );
    }

    /// A slightly different failure scenario from
    /// [consensus_3_nodes_with_failures]. We are not causing the process to
    /// exit but instead forcing nodes to send outdated votes which leads to
    /// them being punished by their peers (via peer score penalties).
    #[tokio::test]
    async fn consensus_3_nodes_outdated_votes_lead_to_peer_score_penalty() {
        const NUM_NODES: usize = 3;
        const READY_TIMEOUT: Duration = Duration::from_secs(20);
        const RUNUP_TIMEOUT: Duration = Duration::from_secs(60);
        const POLL_READY: Duration = Duration::from_millis(500);
        const POLL_HEIGHT: Duration = Duration::from_secs(1);

        let (configs, boot_blocks, stopwatch) = utils::setup(NUM_NODES, true).unwrap();

        let last_valid_height: u64 = boot_blocks + 5;

        let alice_cfg = configs.first().unwrap();
        let mut fgw = FeederGateway::spawn(alice_cfg).unwrap();
        fgw.wait_for_ready(POLL_READY, READY_TIMEOUT).await.unwrap();

        let inject_failure = InjectFailureConfig {
            // Starting from this height..
            height: last_valid_height + 1,
            // ..send outdated votes.
            trigger: InjectFailureTrigger::OutdatedVote,
        };
        // Do this for all three nodes, one of them will be picked to send a proposal
        // at last_valid_height + 1 and the other two will be the sabotaging nodes.
        let mut configs = configs.into_iter().map(|cfg| {
            cfg.with_inject_failure(Some(inject_failure))
                .with_local_feeder_gateway(fgw.port())
                .with_sync_enabled()
        });

        let alice = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();
        alice
            .wait_for_ready(POLL_READY, READY_TIMEOUT)
            .await
            .unwrap();

        let boot_port = alice.consensus_p2p_port();
        let boot_peer_id = utils::read_id_fixture("Alice").unwrap().peer_id;
        let mut configs = configs.map(|cfg| cfg.with_boot_peer(boot_port, boot_peer_id));

        let bob = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();
        let charlie = PathfinderInstance::spawn(configs.next().unwrap()).unwrap();

        let (bob_rdy, charlie_rdy) = tokio::join!(
            bob.wait_for_ready(POLL_READY, READY_TIMEOUT),
            charlie.wait_for_ready(POLL_READY, READY_TIMEOUT)
        );

        bob_rdy.unwrap();
        charlie_rdy.unwrap();

        utils::log_elapsed(stopwatch);

        let (err_tx, err_rx) = mpsc::channel(6);

        // Wait until all three nodes reach `LAST_VALID_HEIGHT`..
        let alice_decided =
            wait_for_height(&alice, last_valid_height, POLL_HEIGHT, None, err_tx.clone());
        let bob_decided =
            wait_for_height(&bob, last_valid_height, POLL_HEIGHT, None, err_tx.clone());
        let charlie_decided = wait_for_height(
            &charlie,
            last_valid_height,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );
        let alice_committed =
            wait_for_block_exists(&alice, last_valid_height, POLL_HEIGHT, true, err_tx.clone());
        let bob_committed =
            wait_for_block_exists(&bob, last_valid_height, POLL_HEIGHT, true, err_tx.clone());
        let charlie_committed = wait_for_block_exists(
            &charlie,
            last_valid_height,
            POLL_HEIGHT,
            true,
            err_tx.clone(),
        );

        utils::join_all(
            vec![
                alice_decided,
                bob_decided,
                charlie_decided,
                alice_committed,
                bob_committed,
                charlie_committed,
            ],
            RUNUP_TIMEOUT,
            err_rx,
        )
        .await
        .unwrap();

        let (err_tx, err_rx) = mpsc::channel(3);

        // ..then wait a bit more for the next height, which should never become decided
        // upon because one of the nodes is sabotaging the consensus network (sending
        // outdated votes) and getting punished by the other two nodes.
        let alice_decided = wait_for_height(
            &alice,
            last_valid_height + 1,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );
        let bob_decided = wait_for_height(
            &bob,
            last_valid_height + 1,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );
        let charlie_decided = wait_for_height(
            &charlie,
            last_valid_height + 1,
            POLL_HEIGHT,
            None,
            err_tx.clone(),
        );

        let err = utils::join_all(
            vec![alice_decided, bob_decided, charlie_decided],
            POLL_HEIGHT * 10,
            err_rx,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("Test timed out"));

        let alice_peer_scores = get_peer_scores(&alice).await.unwrap();
        let bob_peer_scores = get_peer_scores(&bob).await.unwrap();
        let charlie_peer_scores = get_peer_scores(&charlie).await.unwrap();

        let peer_score_penalty_applied = alice_peer_scores
            .iter()
            .any(|score| score.score.as_f64().unwrap() < 0.0)
            || bob_peer_scores
                .iter()
                .any(|score| score.score.as_f64().unwrap() < 0.0)
            || charlie_peer_scores
                .iter()
                .any(|score| score.score.as_f64().unwrap() < 0.0);

        assert!(
            peer_score_penalty_applied,
            "At least one node should have applied a peer score penalty for outdated votes"
        );

        let alice_artifacts = get_cached_artifacts_info(&alice, last_valid_height)
            .await
            .unwrap();
        assert!(
            alice_artifacts.is_empty(),
            "Alice should not have leftover cached consensus data: {alice_artifacts:#?}"
        );

        let bob_artifacts = get_cached_artifacts_info(&bob, last_valid_height)
            .await
            .unwrap();
        assert!(
            bob_artifacts.is_empty(),
            "Bob should not have leftover cached consensus data: {bob_artifacts:#?}"
        );

        let charlie_artifacts = get_cached_artifacts_info(&charlie, last_valid_height)
            .await
            .unwrap();
        assert!(
            charlie_artifacts.is_empty(),
            "Charlie should not have leftover cached consensus data: {charlie_artifacts:#?}"
        );
    }

    async fn get_peer_scores(
        instance: &PathfinderInstance,
    ) -> anyhow::Result<Vec<ApplicationPeerScore>> {
        let rpc_port = instance.rpc_port_watch().1.borrow().1;

        match get_consensus_info(rpc_port).await? {
            JsonRpcReply::Success {
                result:
                    ConsensusInfoOutput {
                        application_peer_scores,
                        ..
                    },
                ..
            } => Ok(application_peer_scores),
            JsonRpcReply::Error { _error, .. } => {
                anyhow::bail!("{:#?}", serde_json::to_string(&_error));
            }
        }
    }
}
