use std::path::PathBuf;

use p2p::preconfirmed::{Client, Event};
use pathfinder_common::ChainId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::p2p::P2PPreconfirmedConfig;

type PreconfirmedHandle = (
    JoinHandle<anyhow::Result<()>>,
    Option<(mpsc::UnboundedReceiver<Event>, Client)>,
);

pub async fn start(
    chain_id: ChainId,
    config: P2PPreconfirmedConfig,
    data_directory: PathBuf,
) -> PreconfirmedHandle {
    inner::start(chain_id, config, data_directory)
        .await
        .unwrap_or_else(|error| {
            (
                tokio::task::spawn(std::future::ready(Err(
                    error.context("Preconfirmed P2P failed to start")
                ))),
                None,
            )
        })
}

#[cfg(feature = "p2p")]
mod inner {
    use std::path::PathBuf;
    use std::time::Duration;

    use anyhow::Context;
    use futures::FutureExt;
    use p2p::libp2p::multiaddr::Protocol;
    use pathfinder_common::ChainId;

    use super::*;
    use crate::config::p2p::P2PPreconfirmedConfig;
    use crate::p2p_network::common::{dial_bootnodes, ensure_peer_id_in_multiaddr};
    use crate::p2p_network::identity;

    #[tracing::instrument(name = "p2p", skip_all)]
    pub(super) async fn start(
        chain_id: ChainId,
        config: P2PPreconfirmedConfig,
        data_directory: PathBuf,
    ) -> anyhow::Result<PreconfirmedHandle> {
        let core_config = p2p::core::Config {
            direct_connection_timeout: config.core.direct_connection_timeout,
            relay_connection_timeout: Duration::from_secs(10),
            max_inbound_direct_peers: config.core.max_inbound_direct_connections,
            max_inbound_relayed_peers: config.core.max_inbound_relayed_connections,
            max_outbound_peers: config.core.max_outbound_connections,
            ip_whitelist: config.core.ip_whitelist,
            bootstrap_period: Some(Duration::from_secs(2 * 60)),
            eviction_timeout: config.core.eviction_timeout,
            inbound_connections_rate_limit: p2p::core::config::RateLimit {
                max: 10,
                interval: Duration::from_secs(1),
            },
            max_read_bytes_per_sec: config.core.max_read_bytes_per_sec,
            max_write_bytes_per_sec: config.core.max_write_bytes_per_sec,
            kad_name: config.core.kad_name,
            data_directory,
        };
        let keypair = identity::load_or_generate(config.core.identity_config_file.clone())
            .context(format!(
                "Loading identity file: {:?}",
                config.core.identity_config_file
            ))?;
        let listen_on = config.core.listen_on;
        let bootstrap_addresses = config.core.bootstrap_addresses;
        let mut predefined_peers = config.core.predefined_peers;

        let my_peer_id = keypair.public().to_peer_id();
        tracing::info!(%my_peer_id, "Preconfirmed network node started with peer ID:");

        // In testing it is convenient to paste the entire list of peers into their
        // configs without having to remove the peer ID of the configured peer.
        if let Some(my_idx) = predefined_peers.iter().position(|addr| {
            addr.iter()
                .any(|p| matches!(p, Protocol::P2p(peer_id) if peer_id == my_peer_id))
        }) {
            predefined_peers.swap_remove(my_idx);
        }

        let (core_client, p2p_events, p2p_main_loop) =
            p2p::new_preconfirmed(keypair, core_config, chain_id);

        let main_loop_handle = { util::task::spawn(p2p_main_loop.run().map(Ok)) };

        for addr in listen_on {
            core_client
                .start_listening(addr.clone())
                .await
                .with_context(|| format!("Starting preconfirmed P2P listener: {addr}"))?;
        }

        if !dial_bootnodes(bootstrap_addresses, &core_client).await {
            anyhow::bail!("Failed to dial any configured bootstrap node")
        }

        for peer in predefined_peers {
            let peer_id =
                ensure_peer_id_in_multiaddr(&peer, "Predefined peers must include peer ID")?;
            core_client
                .dial(peer_id, peer.clone())
                .await
                .context(format!("Dialing predefined peer: {peer}"))?;
        }

        Ok((
            main_loop_handle,
            Some((p2p_events, Client::from(core_client.as_pair()))),
        ))
    }
}

#[cfg(not(feature = "p2p"))]
mod inner {
    use super::*;

    pub(super) async fn start(
        _: ChainId,
        _: P2PPreconfirmedConfig,
        _: PathBuf,
    ) -> anyhow::Result<(
        JoinHandle<anyhow::Result<()>>,
        Option<(mpsc::UnboundedReceiver<Event>, Client)>,
    )> {
        Ok((tokio::task::spawn(futures::future::pending()), None))
    }
}
