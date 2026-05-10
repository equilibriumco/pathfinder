use std::time::Duration;

use p2p::consensus::{Client, HeightAndRound};
use p2p_proto::consensus::{ProposalPart, Vote};
use pathfinder_common::ContractAddress;

/// Configuration for gossip retry behavior.
#[derive(Debug, Clone)]
pub(crate) struct GossipRetryConfig {
    /// Maximum number of retries for recoverable errors.
    pub max_retries: u32,
    /// Maximum number of retries for NoPeersSubscribedToTopic (expected during
    /// startup when no peers are subscribed to the topic yet).
    pub max_no_peers_subscribed_retries: u32,
    /// Initial retry delay in milliseconds for exponential backoff.
    pub initial_retry_delay_ms: u64,
    /// Delay in milliseconds for NoPeersSubscribedToTopic retries (fixed
    /// delay).
    pub no_peers_subscribed_delay_ms: u64,
    /// Maximum exponential backoff delay in milliseconds (cap for backoff).
    pub max_backoff_delay_ms: u64,
}

impl Default for GossipRetryConfig {
    fn default() -> Self {
        Self {
            // Recoverable errors: exponential backoff with fewer retries since these indicate
            // transient network issues that should resolve quickly.
            max_retries: 10,
            // NoPeersSubscribedToTopic: more retries with fixed delay since this is expected
            // during startup when no peers are subscribed to the topic yet. The longer delay (5s)
            // gives peers more time to subscribe before retrying.
            max_no_peers_subscribed_retries: 20,
            initial_retry_delay_ms: 2000,       // 2 seconds
            no_peers_subscribed_delay_ms: 5000, // 5 seconds
            max_backoff_delay_ms: 20_000,       // 20 seconds (2x propose timeout)
        }
    }
}

/// Handler for gossiping messages with retry logic.
pub(crate) struct GossipHandler {
    validator_address: ContractAddress,
    config: GossipRetryConfig,
}

impl GossipHandler {
    /// Create a new gossip retry handler.
    pub fn new(validator_address: ContractAddress, config: GossipRetryConfig) -> Self {
        Self {
            validator_address,
            config,
        }
    }

    /// Gossip a proposal with retry logic.
    pub async fn gossip_proposal(
        &self,
        p2p_client: &Client,
        height_and_round: HeightAndRound,
        proposal_parts: Vec<ProposalPart>,
    ) -> Result<(), anyhow::Error> {
        let context = format!("proposal for {height_and_round}");
        gossip_with_retry(
            self.validator_address,
            &context,
            || {
                let proposal_parts = proposal_parts.clone();
                p2p_client.gossip_proposal(height_and_round, proposal_parts)
            },
            &self.config,
        )
        .await
    }

    /// Gossip a vote with retry logic.
    pub async fn gossip_vote(&self, p2p_client: &Client, vote: Vote) -> Result<(), anyhow::Error> {
        let context = format!("vote {vote:?}");
        gossip_with_retry(
            self.validator_address,
            &context,
            || {
                let vote = vote.clone();
                p2p_client.gossip_vote(vote)
            },
            &self.config,
        )
        .await
    }
}

/// Attempt to gossip a message to the network.
///
/// Recoverable errors are retried with exponential backoff. Fatal errors are
/// returned as an error.
///
/// Note: After max retries for recoverable errors, we return `Ok(())` to avoid
/// crashing the task. The consensus engine has internal timeout mechanisms that
/// should advance rounds if gossip fails. For proposals, if we're the proposer
/// and fail to gossip, the engine should timeout and move to the next round.
/// For votes, other validators can still make progress without our vote.
pub(crate) async fn gossip_with_retry<F, Fut>(
    validator_address: ContractAddress,
    context: &str, // e.g., "proposal" or "vote"
    mut gossip_fn: F,
    config: &GossipRetryConfig,
) -> Result<(), anyhow::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), p2p::libp2p::gossipsub::PublishError>>,
{
    use p2p::libp2p::gossipsub::PublishError;

    let mut retry_count = 0;
    let mut no_peers_subscribed_retry_count = 0;

    loop {
        match gossip_fn().await {
            Ok(()) => {
                tracing::debug!(
                    validator = %validator_address,
                    context = context,
                    "🖧  Gossiping {} SUCCESS",
                    context
                );
                return Ok(());
            }
            // Duplicate means the message was already published, so treat as success.
            Err(PublishError::Duplicate) => {
                tracing::debug!(
                    validator = %validator_address,
                    context = context,
                    "🖧  Gossiping {} SUCCESS (duplicate - already published)",
                    context
                );
                return Ok(());
            }
            // This error variant means "no peers subscribed to the topic" (renamed to
            // NoPeersSubscribedToTopic in newer libp2p versions).
            Err(PublishError::NoPeersSubscribedToTopic) => {
                no_peers_subscribed_retry_count += 1;
                if no_peers_subscribed_retry_count >= config.max_no_peers_subscribed_retries {
                    tracing::error!(
                        validator = %validator_address,
                        context = context,
                        retry_count = no_peers_subscribed_retry_count,
                        max_retries = config.max_no_peers_subscribed_retries,
                        "Failed to gossip {} after max NoPeersSubscribedToTopic retries - giving up",
                        context
                    );
                    // Consensus engine should handle missing gossip via timeouts, so we return Ok.
                    return Ok(());
                }
                tracing::warn!(
                    validator = %validator_address,
                    context = context,
                    retry_count = no_peers_subscribed_retry_count,
                    max_retries = config.max_no_peers_subscribed_retries,
                    "No peers subscribed to topic for {}, retrying...",
                    context
                );
                tokio::time::sleep(Duration::from_millis(config.no_peers_subscribed_delay_ms))
                    .await;
            }
            Err(error) => {
                if is_gossip_error_recoverable(&error) {
                    retry_count += 1;
                    if retry_count >= config.max_retries {
                        tracing::error!(
                            validator = %validator_address,
                            context = context,
                            retry_count = retry_count,
                            max_retries = config.max_retries,
                            error = %error,
                            "Failed to gossip {} after max retries - giving up",
                            context
                        );
                        // Consensus engine should handle missing gossip via timeouts, so we return
                        // Ok.
                        return Ok(());
                    }
                    // Retry with exponential backoff: initial_delay * 2^retry_count (capped at
                    // max_backoff_delay_ms)
                    let backoff_multiplier = 2_u64.pow(retry_count);
                    let delay_ms = (config.initial_retry_delay_ms * backoff_multiplier)
                        .min(config.max_backoff_delay_ms);
                    tracing::warn!(
                        validator = %validator_address,
                        context = context,
                        retry_count = retry_count,
                        max_retries = config.max_retries,
                        delay_ms = delay_ms,
                        error = %error,
                        "Transient error gossiping {} - retrying with exponential backoff",
                        context
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    tracing::error!(
                        validator = %validator_address,
                        context = context,
                        error = %error,
                        "Fatal error gossiping {} - task must restart",
                        context
                    );
                    // Fatal, unexpected publish error. Likely something permanent that won't be
                    // resolved by retrying. Return the error.
                    return Err(anyhow::Error::from(error)
                        .context(format!("Fatal error gossiping {context}")));
                }
            }
        }
    }
}

/// Classify whether a gossip/network error should be retried with exponential
/// backoff.
///
/// Returns `true` for recoverable errors (retried with exponential backoff).
/// Returns `false` for fatal errors (permanent issues, no retries).
pub(crate) fn is_gossip_error_recoverable(error: &p2p::libp2p::gossipsub::PublishError) -> bool {
    use p2p::libp2p::gossipsub::PublishError;

    match error {
        // These are handled separately in gossip_with_retry and should never reach here.
        PublishError::NoPeersSubscribedToTopic => {
            unreachable!("NoPeersSubscribedToTopic handled separately")
        }
        PublishError::Duplicate => unreachable!("Duplicate handled separately"),

        // The network queues are temporarily full but should clear up.
        PublishError::AllQueuesFull(_) => true,

        // IO error during compression
        PublishError::TransformFailed(_) => false,

        // Message will never fit, no point retrying.
        PublishError::MessageTooLarge => false,

        // Signing failed, permanent issue.
        PublishError::SigningError(_) => false,
    }
}
