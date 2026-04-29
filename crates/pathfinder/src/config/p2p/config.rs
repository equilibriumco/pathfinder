use std::num::NonZeroU32;
use std::time::Duration;

use clap::CommandFactory;
use ipnet::IpNet;
use p2p::libp2p::Multiaddr;

use super::cli::{
    P2PConsensusCli,
    P2PConsensusCoreCli,
    P2PPreconfirmedCli,
    P2PPreconfirmedCoreCli,
    P2PSyncCli,
    P2PSyncCoreCli,
};
use crate::config::Cli;

#[derive(Clone)]
pub struct P2PCoreConfig {
    pub identity_config_file: Option<std::path::PathBuf>,
    pub listen_on: Vec<Multiaddr>,
    pub bootstrap_addresses: Vec<Multiaddr>,
    pub predefined_peers: Vec<Multiaddr>,
    pub max_inbound_direct_connections: usize,
    pub max_inbound_relayed_connections: usize,
    pub max_outbound_connections: usize,
    pub ip_whitelist: Vec<IpNet>,
    pub max_read_bytes_per_sec: Option<NonZeroU32>,
    pub max_write_bytes_per_sec: Option<NonZeroU32>,
    pub kad_name: Option<String>,
    pub direct_connection_timeout: Duration,
    pub eviction_timeout: Duration,
}

#[derive(Clone)]
pub struct P2PSyncConfig {
    pub core: P2PCoreConfig,
    pub proxy: bool,
    pub l1_checkpoint_override: Option<pathfinder_ethereum::EthereumStateUpdate>,
    pub stream_timeout: Duration,
    pub response_timeout: Duration,
    pub max_concurrent_streams: usize,
}

#[derive(Clone)]
pub struct P2PConsensusConfig {
    pub core: P2PCoreConfig,
}

#[derive(Clone)]
pub struct P2PPreconfirmedConfig {
    pub core: P2PCoreConfig,
    pub enable: bool,
}

/// Generates an `impl From` implementation for a given `target` that converts
/// a `P2PTargetCoreCli` struct into a `P2PCoreConfig` struct.
macro_rules! impl_from_p2p_cli {
    ($target:literal) => {
        paste::paste! {
            impl From<[<P2P $target:camel CoreCli>]> for P2PCoreConfig {
                fn from(cli: [<P2P $target:camel CoreCli>]) -> Self {
                    use std::str::FromStr;

                    use clap::error::ErrorKind;
                    use p2p::libp2p::multiaddr::Result;

                    let parse_multiaddr_vec = |field: &str, multiaddrs: Vec<String>| -> Vec<Multiaddr> {
                        multiaddrs
                            .into_iter()
                            .map(|addr| Multiaddr::from_str(addr.trim()))
                            .collect::<Result<Vec<_>>>()
                            .unwrap_or_else(|error| {
                                Cli::command()
                                    .error(ErrorKind::ValueValidation, format!("{field}: {error}"))
                                    .exit()
                            })
                    };

                    if (1..25).contains(&cli.[<$target:lower _max_inbound_direct_connections>]) {
                        Cli::command()
                            .error(
                                ErrorKind::ValueValidation,
                                concat!("p2p.", stringify!($target:lower), ".max-inbound-direct-connections must be zero or at least 25"),
                            )
                            .exit()
                    }

                    if (1..25).contains(&cli.[<$target:lower _max_inbound_relayed_connections>]) {
                        Cli::command()
                            .error(
                                ErrorKind::ValueValidation,
                                concat!("p2p.", stringify!($target:lower), ".max-inbound-relayed-connections must be zero or at least 25"),
                            )
                            .exit()
                    }

                    // The low watermark is defined in `bootstrap_on_low_peers`
                    // https://github.com/libp2p/rust-libp2p/blob/d7beb55f672dce54017fa4b30f67ecb8d66b9810/protocols/kad/src/behaviour.rs#L1401).
                    // as the K value of 20
                    // https://github.com/libp2p/rust-libp2p/blob/d7beb55f672dce54017fa4b30f67ecb8d66b9810/protocols/kad/src/lib.rs#L93
                    if cli.[<$target:lower _max_outbound_connections>] <= 20 {
                        Cli::command()
                            .error(
                                ErrorKind::ValueValidation,
                                concat!("p2p.", stringify!($target:lower), ".max-outbound-connections must be at least 21"),
                            )
                            .exit()
                    }

                    if cli.[<$target:lower _kad_name>].iter().any(|x| !x.starts_with('/')) {
                        Cli::command()
                            .error(
                                ErrorKind::ValueValidation,
                                concat!("each item in p2p.", stringify!($target:lower), ".experimental.kad-names must start with '/'"),
                            )
                            .exit()
                    }

                    if cli.[<$target:lower _max_read_bytes_per_sec>].is_some_and(|max_read| max_read < p2p::TRANSPORT_MIN_BYTES_PER_SEC) ||
                          cli.[<$target:lower _max_write_bytes_per_sec>].is_some_and(|max_write| max_write < p2p::TRANSPORT_MIN_BYTES_PER_SEC) {
                        Cli::command()
                            .error(ErrorKind::ValueValidation, format!(
                            "P2P IO rate limit must be at least {}

Note: this is the minimum value that should provide normal P2P behaviour and avoid timeouts",
                            p2p::TRANSPORT_MIN_BYTES_PER_SEC_PRETTY
                        ))
                        .exit()
                    }

                    Self {
                        identity_config_file: cli.[< $target:lower _identity_config_file>],
                        listen_on: parse_multiaddr_vec(concat!("p2p.", stringify!($target:lower), ".listen-on"), cli.[<$target:lower _listen_on>]),
                        bootstrap_addresses: parse_multiaddr_vec(
                            concat!("p2p.", stringify!($target:lower), ".bootstrap-addresses"),
                            cli.[<$target:lower _bootstrap_addresses>],
                        ),
                        predefined_peers: parse_multiaddr_vec(
                            concat!("p2p.", stringify!($target:lower), ".predefined-peers"),
                            cli.[<$target:lower _predefined_peers>],
                        ),
                        max_inbound_direct_connections: cli.[<$target:lower _max_inbound_direct_connections>] as usize,
                        max_inbound_relayed_connections: cli.[<$target:lower _max_inbound_relayed_connections>] as usize,
                        max_outbound_connections: cli.[<$target:lower _max_outbound_connections>] as usize,
                        ip_whitelist: cli.[<$target:lower _ip_whitelist>],
                        kad_name: cli.[<$target:lower _kad_name>],
                        direct_connection_timeout: Duration::from_secs(cli.[<$target:lower _direct_connection_timeout>].into()),
                        eviction_timeout: Duration::from_secs(cli.[<$target:lower _eviction_timeout>].into()),
                        max_read_bytes_per_sec: cli.[<$target:lower _max_read_bytes_per_sec>],
                        max_write_bytes_per_sec: cli.[<$target:lower _max_write_bytes_per_sec>],
                    }
                }
            }
        }
    };
}

impl_from_p2p_cli!("sync");
impl_from_p2p_cli!("consensus");
impl_from_p2p_cli!("preconfirmed");

impl P2PSyncConfig {
    pub(crate) fn parse_or_exit(args: P2PSyncCli) -> Self {
        Self {
            // SAFETY: core conversion is safe because we exit the process on error
            core: args.core.into(),
            proxy: args.proxy,
            l1_checkpoint_override: parse_l1_checkpoint_or_exit(args.l1_checkpoint_override),
            stream_timeout: Duration::from_secs(args.stream_timeout.into()),
            response_timeout: Duration::from_secs(args.response_timeout.into()),
            max_concurrent_streams: args.max_concurrent_streams,
        }
    }
}

fn parse_l1_checkpoint_or_exit(
    l1_checkpoint_override: Option<String>,
) -> Option<pathfinder_ethereum::EthereumStateUpdate> {
    use clap::error::ErrorKind;
    use pathfinder_common::{BlockHash, BlockNumber, StateCommitment};

    #[derive(serde::Deserialize)]
    struct Dto {
        state_root: StateCommitment,
        block_number: BlockNumber,
        block_hash: BlockHash,
    }

    fn exit_now(e: impl std::fmt::Display) {
        Cli::command()
            .error(
                ErrorKind::ValueValidation,
                format!("p2p.experimental.l1-checkpoint-override: {e}"),
            )
            .exit()
    }

    l1_checkpoint_override.map(|f| {
        // SAFETY: unwraps are safe because we exit the process on error
        let f = std::fs::File::open(f).map_err(exit_now).unwrap();
        let dto: Dto = serde_json::from_reader(f).map_err(exit_now).unwrap();
        pathfinder_ethereum::EthereumStateUpdate {
            state_root: dto.state_root,
            block_number: dto.block_number,
            block_hash: dto.block_hash,
        }
    })
}

impl P2PConsensusConfig {
    pub fn parse_or_exit(args: P2PConsensusCli) -> Self {
        Self {
            // SAFETY: core conversion is safe because we exit the process on error
            core: args.core.into(),
        }
    }
}

impl P2PPreconfirmedConfig {
    pub fn parse_or_exit(args: P2PPreconfirmedCli) -> Self {
        Self {
            // SAFETY: core conversion is safe because we exit the process on error
            core: args.core.into(),
            enable: args.enable,
        }
    }
}
