use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use backon::Retryable;
use futures::future::join_all;
use p2p::core::Client;
use p2p::libp2p::multiaddr::Protocol;
use p2p::libp2p::{Multiaddr, PeerId};

pub async fn dial_bootnodes<C>(
    bootstrap_addresses: Vec<Multiaddr>,
    core_client: &Client<C>,
) -> bool {
    if bootstrap_addresses.is_empty() {
        return true;
    }

    let mut bootstrap_addrs_by_peer: HashMap<PeerId, Vec<Multiaddr>> = HashMap::new();

    for bootstrap_address in bootstrap_addresses {
        let peer_id = match ensure_peer_id_in_multiaddr(
            &bootstrap_address,
            "Bootstrap addresses must include peer ID",
        ) {
            Ok(id) => id,
            Err(error) => {
                tracing::warn!(?error, "Invalid bootstrap address {bootstrap_address}");
                continue;
            }
        };

        bootstrap_addrs_by_peer
            .entry(peer_id)
            .or_default()
            .push(bootstrap_address);
    }

    let backoff = backon::ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(1))
        .with_max_times(5);

    let dials =
        bootstrap_addrs_by_peer
            .into_iter()
            .map(|(peer_id, bootstrap_addresses)| async move {
                let dial_result = (|| async {
                    core_client
                        .dial_many(peer_id, bootstrap_addresses.clone())
                        .await
                })
                .retry(backoff)
                .notify(|error, timeout| {
                    tracing::warn!(
                        ?bootstrap_addresses,
                        %error,
                        ?timeout,
                        "Failed to dial bootstrap node, retrying",
                    );
                })
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        ?bootstrap_addresses,
                        %error,
                        "Failed to dial bootstrap node",
                    );
                });

                for address in bootstrap_addresses {
                    let relay_listener_address = address.with(Protocol::P2pCircuit);
                    if let Err(error) = core_client
                        .start_listening(relay_listener_address.clone())
                        .await
                    {
                        tracing::warn!(
                            ?error,
                            "Failed starting relay listener on {relay_listener_address}"
                        );
                    }
                }

                dial_result.is_ok()
            });

    join_all(dials).await.into_iter().any(|success| success)
}

pub fn ensure_peer_id_in_multiaddr(addr: &Multiaddr, msg: &'static str) -> anyhow::Result<PeerId> {
    addr.iter()
        .find_map(|p| match p {
            Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
        .context(msg)
}
