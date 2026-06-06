use std::num::NonZeroU32;

use ipnet::IpNet;

/// For a given `target`, this macro defines a `P2PTargetCoreCli` struct that
/// contains all the core p2p CLI options, where each CLI option is prefixed
/// with `p2p.target.` and each environment variable is prefixed with
/// `PATHFINDER_P2P_TARGET_`. Moreover each struct member is also prefixed with
/// `target_`, because this struct is ultimately flattened by clap, otherwise we
/// would get "Command Pathfinder: Argument names must be unique, but
/// 'field' is in use by more than one argument or group" error.
macro_rules! define_p2p_core_cli {
    ($target:literal) => {
        paste::paste! {
            #[derive(clap::Args)]
            pub(super) struct [<P2P $target:camel CoreCli>] {
                #[arg(
                    long = "p2p." $target:lower ".identity-config-file",
                    long_help = "Path to file containing the private key of the node. If not \
                                provided, a new random key will be generated.",
                    value_name = "PATH",
                    env = "PATHFINDER_P2P_" $target:upper "_IDENTITY_CONFIG_FILE"
                )]
                pub [<$target:lower _identity_config_file>]: Option<std::path::PathBuf>,

                #[arg(
                    long = "p2p." $target:lower ".listen-on",
                    long_help = "The list of multiaddresses on which to listen for incoming p2p connections. \
                                If not provided, default route on randomly assigned port will be used.",
                    value_name = "MULTIADDRESS_LIST",
                    value_delimiter = ',',
                    default_value = "/ip4/0.0.0.0/tcp/0",
                    env = "PATHFINDER_P2P_" $target:upper "_LISTEN_ON"
                )]
                pub [<$target:lower _listen_on>]: Vec<String>,

                #[arg(
                    long = "p2p." $target:lower ".bootstrap-addresses",
                    long_help = r#"Comma separated list of multiaddresses to use as bootstrap nodes. Each multiaddress must contain a peer ID.

Example:
    '/ip4/127.0.0.1/9001/p2p/12D3KooWBEkKyufuqCMoZLRhVzq4xdHxVWhhYeBpjw92GSyZ6xaN,/ip4/127.0.0.1/9002/p2p/12D3KooWBEkKyufuqCMoZLRhVzq4xdHxVWhhYeBpjw92GSyZ6xaN'"#,
                    value_name = "MULTIADDRESS_LIST",
                    value_delimiter = ',',
                    env = "PATHFINDER_P2P_" $target:upper "_BOOTSTRAP_ADDRESSES"
                )]
                pub [<$target:lower _bootstrap_addresses>]: Vec<String>,

                #[arg(
                    long = "p2p." $target:lower ".predefined-peers",
                    long_help = r#"Comma separated list of multiaddresses to use as peers apart from peers discovered via DHT discovery. Each multiaddress must contain a peer ID.

Example:
    '/ip4/127.0.0.1/9003/p2p/12D3KooWBEkKyufuqCMoZLRhVzq4xdHxVWhhYeBpjw92GSyZ6xaP,/ip4/127.0.0.1/9004/p2p/12D3KooWBEkKyufuqCMoZLRhVzq4xdHxVWhhYeBpjw92GSyZ6xaR'"#,
                    value_name = "MULTIADDRESS_LIST",
                    value_delimiter = ',',
                    env = "PATHFINDER_P2P_" $target:upper "_PREDEFINED_PEERS"
                )]
                pub [<$target:lower _predefined_peers>]: Vec<String>,

                #[arg(
                    long = "p2p." $target:lower ".max-inbound-direct-connections",
                    long_help = "The maximum number of inbound direct (non-relayed) connections.",
                    value_name = "MAX_INBOUND_DIRECT_CONNECTIONS",
                    env = "PATHFINDER_P2P_" $target:upper "_MAX_INBOUND_DIRECT_CONNECTIONS",
                    default_value = "30"
                )]
                pub [<$target:lower _max_inbound_direct_connections>]: u32,

                #[arg(
                    long = "p2p." $target:lower ".max-inbound-relayed-connections",
                    long_help = "The maximum number of inbound relayed connections.",
                    value_name = "MAX_INBOUND_RELAYED_CONNECTIONS",
                    env = "PATHFINDER_P2P_" $target:upper "_MAX_INBOUND_RELAYED_CONNECTIONS",
                    default_value = "30"
                )]
                pub [<$target:lower _max_inbound_relayed_connections>]: u32,

                #[arg(
                    long = "p2p." $target:lower ".max-outbound-connections",
                    long_help = "The maximum number of outbound connections.",
                    value_name = "MAX_OUTBOUND_CONNECTIONS",
                    env = "PATHFINDER_P2P_" $target:upper "_MAX_OUTBOUND_CONNECTIONS",
                    default_value = "50"
                )]
                pub [<$target:lower _max_outbound_connections>]: u32,

                #[arg(
                    long = "p2p." $target:lower ".ip-whitelist",
                    long_help = "Comma separated list of IP addresses or IP address ranges (in CIDR) to \
                                whitelist for incoming connections. If not provided, all incoming \
                                connections are allowed.",
                    value_name = "LIST",
                    default_value = "0.0.0.0/0,::/0",
                    value_delimiter = ',',
                    env = "PATHFINDER_P2P_" $target:upper "_IP_WHITELIST"
                )]
                pub [<$target:lower _ip_whitelist>]: Vec<IpNet>,

                #[arg(
                    long = "p2p." $target:lower ".experimental.kad-name",
                    long_help = "Custom Kademlia protocol name.",
                    value_name = "PROTOCOL_NAME",
                    env = "PATHFINDER_P2P_" $target:upper "_EXPERIMENTAL_KAD_NAME"
                )]
                pub [<$target:lower _kad_name>]: Option<String>,

                #[arg(
                    long = "p2p." $target:lower ".experimental.direct-connection-timeout",
                    long_help = "A direct (not relayed) peer can only connect once in this period.",
                    value_name = "SECONDS",
                    default_value = "30",
                    env = "PATHFINDER_P2P_" $target:upper "_EXPERIMENTAL_DIRECT_CONNECTION_TIMEOUT"
                )]
                pub [<$target:lower _direct_connection_timeout>]: u32,

                #[arg(
                    long = "p2p." $target:lower ".experimental.eviction-timeout",
                    long_help = "How long to prevent evicted peers from reconnecting.",
                    value_name = "SECONDS",
                    default_value = "900",
                    env = "PATHFINDER_P2P_" $target:upper "_EXPERIMENTAL_EVICTION_TIMEOUT"
                )]
                pub [<$target:lower _eviction_timeout>]: u32,

                #[arg(
                    long = "p2p." $target:lower ".experimental.max-read-bytes-per-sec",
                    long_help = "Maximum read rate per connection in bytes per second.",
                    value_name = "BYTES_PER_SECOND",
                    env = "PATHFINDER_P2P_" $target:upper "_EXPERIMENTAL_MAX_READ_BYTES_PER_SEC",
                    requires = $target:lower "_max_write_bytes_per_sec"
                )]
                pub [<$target:lower _max_read_bytes_per_sec>]: Option<NonZeroU32>,

                #[arg(
                    long = "p2p." $target:lower ".experimental.max-write-bytes-per-sec",
                    long_help = "Maximum write rate per connection in bytes per second.",
                    value_name = "BYTES_PER_SECOND",
                    env = "PATHFINDER_P2P_" $target:upper "_EXPERIMENTAL_MAX_WRITE_BYTES_PER_SEC",
                    requires = $target:lower "_max_read_bytes_per_sec"
                )]
                pub [<$target:lower _max_write_bytes_per_sec>]: Option<NonZeroU32>,
            }
        }
    };
}

define_p2p_core_cli! {"sync"}
define_p2p_core_cli! {"consensus"}

#[derive(clap::Args)]
pub(crate) struct P2PSyncCli {
    #[clap(flatten)]
    pub(super) core: P2PSyncCoreCli,

    #[arg(
        long = "p2p.sync.proxy",
        long_help = "Enable syncing from feeder gateway and proxy to p2p network. Otherwise sync from p2p network, which is the default.",
        default_value = "false",
        action = clap::ArgAction::Set,
        env = "PATHFINDER_P2P_SYNC_PROXY"
    )]
    pub proxy: bool,

    #[arg(
        long = "p2p.sync.experimental.l1-checkpoint-override-json-path",
        long_help = "Override L1 sync checkpoint retrieved from the Ethereum API. This option \
                     points to a json encoded file containing an L1 checkpoint from which \
                     pathfinder will sync backwards till genesis before switching to syncing \
                     forward and following the head of the chain. Example contents: { \
                     \"block_hash\": \"0x1\", \"block_number\": 2, \"state_root\": \"0x3\" }",
        value_name = "JSON_FILE",
        env = "PATHFINDER_P2P_EXPERIMENTAL_L1_CHECKPOINT_OVERRIDE"
    )]
    pub l1_checkpoint_override: Option<String>,

    #[arg(
        long = "p2p.sync.experimental.stream-timeout",
        long_help = "Timeout of the entire stream in the request/response-stream protocol.",
        value_name = "SECONDS",
        default_value = "60",
        env = "PATHFINDER_P2P_EXPERIMENTAL_STREAM_TIMEOUT"
    )]
    pub stream_timeout: u32,

    #[arg(
        long = "p2p.sync.experimental.response-timeout",
        long_help = "Timeout of a single response in the request/response-stream protocol.",
        value_name = "SECONDS",
        default_value = "10",
        env = "PATHFINDER_P2P_EXPERIMENTAL_RESPONSE_TIMEOUT"
    )]
    pub response_timeout: u32,

    #[arg(
        long = "p2p.sync.experimental.max-concurrent-streams",
        long_help = "Maximum allowed number of concurrent streams per each \
                     request/response-stream protocol per connection.",
        value_name = "LIMIT",
        default_value = "100",
        env = "PATHFINDER_P2P_EXPERIMENTAL_MAX_CONCURRENT_STREAMS"
    )]
    pub max_concurrent_streams: usize,
}

#[derive(clap::Args)]
pub struct P2PConsensusCli {
    #[clap(flatten)]
    pub(super) core: P2PConsensusCoreCli,
}
