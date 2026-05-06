//! This task:
//! 1. handles requests from the consensus engine, for example to propose a new
//!    block
//! 2. handles requests from the P2P task, for example a vote or a proposal has
//!    been received and needs to be processed by the consensus engine
//! 3. issues commands to the consensus engine, for example to start a new
//!    height, or a vote has been received from the P2P network and needs to be
//!    processed by the consensus engine
//! 4. issues commands to the P2P task, for example to gossip a proposal or a
//!    vote

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use p2p::consensus::HeightAndRound;
use p2p_proto::consensus::{ProposalFin, ProposalPart};
use pathfinder_common::{BlockId, BlockNumber, ContractAddress, ProposalCommitment};
use pathfinder_consensus::{
    Config,
    Consensus,
    ConsensusCommand,
    ConsensusEvent,
    Proposal,
    Round,
    ValidatorSet,
    ValidatorSetProvider,
};
use pathfinder_storage::Storage;
use tokio::sync::mpsc;

use super::fetch_proposers::L2ProposerSelector;
use super::fetch_validators::L2ValidatorSetProvider;
use super::{integration_testing, ConsensusTaskEvent, ConsensusValue, P2PTaskEvent};
use crate::config::integration_testing::InjectFailureConfig;
use crate::config::ConsensusConfig;
use crate::consensus::dummy_proposal;
use crate::devnet::Account;

#[allow(clippy::too_many_arguments)]
pub fn spawn(
    chain_id: pathfinder_common::ChainId,
    config: ConsensusConfig,
    wal_directory: PathBuf,
    tx_to_p2p: mpsc::Sender<P2PTaskEvent>,
    mut rx_from_p2p: mpsc::Receiver<ConsensusTaskEvent>,
    main_storage: Storage,
    data_directory: &Path,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    // Does nothing in production builds. Used for integration testing only.
    inject_failure: Option<InjectFailureConfig>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let data_directory = data_directory.to_path_buf();

    util::task::spawn(async move {
        let main_storage_clone = main_storage.clone();
        let account = util::task::spawn_blocking(move |_| {
            let mut db_conn = main_storage_clone.connection()?;
            let db_txn = db_conn.transaction()?;
            Account::from_storage(&db_txn)
        })
        .await??;

        let highest_committed = highest_committed(&main_storage)
            .context("Failed to read highest committed block at startup")?;
        // Get the validator address and validator set provider
        let validator_address = config.my_validator_address;
        let validator_set_provider =
            L2ValidatorSetProvider::new(main_storage.clone(), chain_id, config.clone());

        // Get the proposer selector
        let proposer_selector =
            L2ProposerSelector::new(main_storage.clone(), chain_id, config.clone());

        let mut consensus =
            Consensus::<ConsensusValue, ContractAddress, L2ProposerSelector>::recover_with_proposal_selector(
                Config::new(validator_address)
                    .with_history_depth(config.history_depth)
                    .with_wal_dir(wal_directory),
                // TODO use a dynamic validator set provider, once fetching the validator set from
                // the staking contract is implemented. Related issue: https://github.com/equilibriumco/pathfinder/issues/2936
                Arc::new(validator_set_provider.clone()),
                proposer_selector,
                highest_committed,
            )?;

        {
            // Compute the next height to work on using all available information:
            // - max_active_height: highest incomplete/active height being tracked
            // - last_decided_height: highest decided height (even if not actively tracked)
            // - highest_committed + 1: next height after what's been committed to main DB
            let next_height = [
                consensus.max_active_height().unwrap_or(0),
                consensus.last_decided_height().unwrap_or(0),
                highest_committed.map(|h| h + 1).unwrap_or(0),
            ]
            .into_iter()
            .max()
            .unwrap_or(0);
            let next_height = BlockNumber::new(next_height)
                .context("Next height exceeds i64::MAX")?
                .get();

            tracing::trace!(%next_height, "consensus task started with");

            start_height_if_inactive(
                &mut consensus,
                next_height,
                validator_set_provider
                    .get_validator_set(next_height)
                    .context("Failed to get validator set at startup")?,
            );

            // The explicit code block is to prohibit reusing `next_height`
        }

        loop {
            let consensus_task_event = tokio::select! {
                consensus_event = consensus.next_event() => {
                    match consensus_event {
                        Some(event) => ConsensusTaskEvent::Event(event),
                        None => {
                            continue;
                        }
                    }
                }
                from_p2p = rx_from_p2p.recv() => {
                    match from_p2p {
                        Some(command) => command,
                        None => {
                            tracing::warn!("Receiver for commands from the P2P task was dropped, exiting Consensus task");
                            anyhow::bail!("Receiver for commands from the P2P task was dropped, exiting Consensus task");
                        }
                    }
                }
            };

            match consensus_task_event {
                ConsensusTaskEvent::Event(event) => {
                    tracing::info!("🧠 ℹ️  {validator_address} consensus event: {event:?}");

                    match event {
                        // The consensus engine wants us to propose a block for the given height and
                        // round. We create a proposal, feed its commitment back to the engine, and
                        // cache the proposal for gossiping when the engine requests so.
                        ConsensusEvent::RequestProposal { height, round, .. } => {
                            tracing::info!(
                                "🧠 🔍 {validator_address} is proposing at height {height}, round \
                                 {round}",
                            );

                            dummy_proposal::wait_for_parent_committed(
                                height,
                                main_storage.clone(),
                                Duration::from_millis(50),
                            )
                            .await
                            .context("Waiting for parent block to be committed")?;

                            match dummy_proposal::create(
                                height,
                                round.into(),
                                &account,
                                validator_address,
                                main_storage.clone(),
                                compiler_resource_limits,
                                blockifier_libfuncs,
                            ) {
                                Ok((wire_proposal, finalized_block)) => {
                                    let ProposalFin {
                                        proposal_commitment,
                                        executed_transaction_count: _,
                                        fin_payload: _,
                                    } = wire_proposal
                                        .last()
                                        .and_then(ProposalPart::as_fin)
                                        .context(format!(
                                            "Proposal for height {height} round {round} is \
                                             missing ProposalFin part"
                                        ))?;

                                    let value =
                                        ConsensusValue(ProposalCommitment(proposal_commitment.0));

                                    tx_to_p2p
                                        .send(P2PTaskEvent::CacheProposal(
                                            HeightAndRound::new(height, round),
                                            wire_proposal,
                                            finalized_block,
                                        ))
                                        .await
                                        .expect("Cache proposal receiver not to be dropped");

                                    let proposal = Proposal {
                                        height,
                                        round: round.into(),
                                        proposer: validator_address,
                                        pol_round: Round::nil(),
                                        value,
                                    };

                                    tracing::info!(
                                        "🧠 ⚙️  {validator_address} handling command \
                                         Propose({proposal:?})"
                                    );

                                    consensus.handle_command(ConsensusCommand::Propose(proposal));
                                }
                                Err(e) => {
                                    // Proposal creation failed - skip this round but continue
                                    // consensus (we can still vote on other validators' proposals)
                                    //
                                    // NOTE: The consensus engine is event-driven and doesn't block
                                    // waiting for our proposal. If we're the designated proposer
                                    // and don't propose, the round will timeout and move to the
                                    // next round.
                                    tracing::warn!(
                                        validator = %validator_address,
                                        height = height,
                                        round = round,
                                        error = %e,
                                        "Failed to create proposal - skipping this round."
                                    );
                                }
                            }
                        }
                        // The consensus engine wants us to gossip a message via the P2P consensus
                        // network.
                        ConsensusEvent::Gossip(msg) => {
                            // Note: sometimes the engine will request gossiping votes for heights
                            // lower than the current height, due to the fact that `history_depth`
                            // in config is > 0 and we're not supporting round certificates yet. We
                            // do want to gossip those votes, because rarely this could even cause
                            // the network to stall with us having decided upon H while the others
                            // not due to subtle race conditions and our votes missing in their
                            // consensus engines.
                            tx_to_p2p
                                .send(P2PTaskEvent::GossipRequest(msg))
                                .await
                                .expect("Gossip request receiver not to be dropped");
                        }
                        // Consensus has been reached for the given height and value.
                        ConsensusEvent::Decision {
                            height,
                            round,
                            value,
                        } => {
                            tracing::info!(
                                "🧠 ✅ {validator_address} decided on {value} at height {height} \
                                 round {round}",
                            );

                            // Does nothing in production builds.
                            integration_testing::debug_fail_on_decided(
                                height,
                                inject_failure,
                                &data_directory,
                            );

                            let height_and_round = HeightAndRound::new(height, round);

                            tx_to_p2p
                                .send(P2PTaskEvent::MarkBlockAsDecidedAndCleanUp(
                                    height_and_round,
                                    value.clone(),
                                ))
                                .await
                                .expect("Commit block receiver not to be dropped");

                            // Start the next height if:
                            // - this decision's height is the latest (highest) decided height by
                            //   consensus (otherwise this is not the highest decided height due to
                            //   some race conditions),
                            // - AND there is no active height that is larger than this last decided
                            //   height (otherwise the next height has already been started and is
                            //   actively being worked on).
                            if consensus
                                .last_decided_height()
                                .map_or(true, |last_decided| last_decided == height)
                                && consensus
                                    .max_active_height()
                                    .map_or(true, |max_active| max_active <= height)
                            {
                                let next_height = BlockNumber::new(height + 1)
                                    .context("Next height exceeds i64::MAX")?
                                    .get();

                                start_height_if_inactive(
                                    &mut consensus,
                                    next_height,
                                    validator_set_provider
                                        .get_validator_set(next_height)
                                        .context("Failed to get validator set")?,
                                );
                            }
                        }
                        ConsensusEvent::Error(error) => {
                            if error.is_recoverable() {
                                // Recoverable errors: log and continue
                                // - WAL entry errors: can skip corrupted entries
                                // - Invalid peer messages: engine should handle, we continue
                                tracing::warn!(
                                    validator = %validator_address,
                                    error = %error,
                                    error_chain = %format!("{:#}", error),
                                    "Recoverable consensus error - continuing operation"
                                );
                                // Continue to next event - don't restart task
                            } else {
                                tracing::error!(
                                    validator = %validator_address,
                                    error = %error,
                                    error_chain = %format!("{:#}", error),
                                    "Fatal consensus error"
                                );
                                // Bail out, stop the consensus
                                return Err(
                                    anyhow::Error::from(error).context("Fatal consensus error")
                                );
                            }
                        }
                    }
                }
                ConsensusTaskEvent::CommandFromP2P(cmd) => {
                    tracing::info!("🧠 ⚙️  {validator_address} handling command {cmd:?}");

                    let cmd_height = cmd.height();
                    match &cmd {
                        // There were no p2p messages for a height higher than the current height,
                        // so we did start a new height upon successful decision, before any p2p
                        // messages for the new height were received.
                        ConsensusCommand::StartHeight(..) | ConsensusCommand::Propose(_) => {
                            // Commands from P2P should always be for current or
                            // future heights.
                            let next_height = consensus
                                .max_active_height()
                                .unwrap_or_default()
                                .max(consensus.last_decided_height().unwrap_or_default() + 1);
                            debug_assert!(
                                cmd_height >= next_height,
                                "Received command for height {cmd_height} < current height \
                                 {next_height}"
                            );
                        }
                        // Sometimes messages for the next height are received before the engine
                        // decides upon the current height. In such case we need to ensure that a
                        // consensus engine is already started for this new height carried in those
                        // messages.
                        ConsensusCommand::Proposal(_) | ConsensusCommand::Vote(_) => {
                            if let ConsensusCommand::Vote(ref signed_vote) = cmd {
                                let vote = &signed_vote.vote;
                                // The condition is always false in production builds.
                                if integration_testing::debug_ignore_received_vote(
                                    vote.r#type.clone(),
                                    vote.height,
                                    vote.round.as_u32(),
                                    inject_failure,
                                ) {
                                    continue;
                                }
                            }

                            start_height_if_inactive(
                                &mut consensus,
                                cmd_height,
                                validator_set_provider
                                    .get_validator_set(cmd_height)
                                    .context("Failed to get validator set")?,
                            );
                        }
                    }

                    consensus.handle_command(cmd);
                }
            }

            // Malachite is coroutine based, otherwise we starve other futures
            // in the outer select.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
}

/// Reads the highest committed block number from main storage.
fn highest_committed(main_storage: &Storage) -> anyhow::Result<Option<u64>> {
    let mut db_conn = main_storage
        .connection()
        .context("Failed to create database connection for reading highest committed block")?;
    let db_txn = db_conn
        .transaction()
        .context("Failed to create database transaction for reading highest committed block")?;
    let highest_committed = db_txn
        .block_number(BlockId::Latest)
        .context("Failed to query latest block number")?
        .map(|x| x.get());
    Ok(highest_committed)
}

/// Starts consensus for the given height if not already active.
fn start_height_if_inactive(
    consensus: &mut Consensus<ConsensusValue, ContractAddress, L2ProposerSelector>,
    height: u64,
    validator_set: ValidatorSet<ContractAddress>,
) {
    if !consensus.is_height_active(height) {
        tracing::trace!(%height, "🧠 🚀  Starting consensus for");
        consensus.handle_command(ConsensusCommand::StartHeight(height, validator_set));
    } else {
        tracing::trace!(%height, "🧠 🤷  Consensus already active for");
    }
}
