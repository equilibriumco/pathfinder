use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use clap::{ArgAction, CommandFactory, Parser};
#[cfg(feature = "p2p")]
use pathfinder_common::ContractAddress;
use pathfinder_common::{AllowedOrigins, StarknetVersion};
#[cfg(feature = "p2p")]
use pathfinder_crypto::Felt;
use pathfinder_executor::{VersionedConstants, VersionedConstantsMap};
use pathfinder_storage::JournalMode;
use reqwest::Url;
use util::percentage::Percentage;

pub mod integration_testing;
pub mod p2p;

#[cfg(feature = "p2p")]
use p2p::cli::{P2PConsensusCli, P2PPreconfirmedCli, P2PSyncCli};
use p2p::{P2PConsensusConfig, P2PPreconfirmedConfig, P2PSyncConfig};

const COMPILER_MEMORY_USAGE_ALLOWED_RANGE: std::ops::RangeInclusive<u64> =
    (pathfinder_compiler::ResourceLimits::RECOMMENDED_MEMORY_USAGE_LIMIT_MIB / 2)
        ..=(4 * pathfinder_compiler::ResourceLimits::RECOMMENDED_MEMORY_USAGE_LIMIT_MIB);

const COMPILER_CPU_TIME_ALLOWED_RANGE: std::ops::RangeInclusive<u64> =
    pathfinder_compiler::ResourceLimits::RECOMMENDED_CPU_TIME_LIMIT
        ..=(4 * pathfinder_compiler::ResourceLimits::RECOMMENDED_CPU_TIME_LIMIT);

#[derive(Parser)]
#[command(name = "Pathfinder")]
#[command(author = "Equilibrium Labs")]
#[command(version = pathfinder_version::VERSION)]
#[command(propagate_version = true)]
#[command(
    about = "A Starknet node implemented by Equilibrium Labs. Submit bug reports and issues at https://github.com/equilibriumco/pathfinder."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Parse command line arguments, defaulting to the `node` subcommand
/// if no valid subcommand is provided.
///
/// NOTE: There are nicer ways to do this but they all involve setting
/// the `#[command(flatten)]` attribute on [`NodeArgs`] which isn't possible
/// as long as it has fields with `#[clap(skip)]`.
pub fn parse_cli() -> Cli {
    let mut os_args: Vec<_> = std::env::args_os().collect();
    let arg1 = os_args.get(1).and_then(|arg1| arg1.to_str());

    // If a valid subcommand was provided, run it. Otherwise, default to the
    // `node` subcommand and let clap handle any errors.
    match arg1 {
        Some(arg1) if CommandKind::from_str(arg1).is_ok() => {}
        _ => {
            os_args.insert(1, "node".into());
        }
    }

    Cli::parse_from(os_args)
}

#[derive(clap::Subcommand)]
pub enum Command {
    /// Run the Pathfinder node.
    Node(Box<NodeArgs>),

    /// Run the Sierra to CASM compiler. Raw Sierra class definitions are read
    /// from stdin and the compiled CASM is written to stdout.
    ///
    /// This command is intended to be used as a subprocess by the main
    /// `pathfinder` executable and is not generally useful to run directly.
    Compile(CompileConfig),
}

enum CommandKind {
    Node,
    Compile,
    Help,
    Version,
}

impl FromStr for CommandKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "node" => Ok(CommandKind::Node),
            "compile" => Ok(CommandKind::Compile),
            "help" | "--help" | "-h" => Ok(CommandKind::Help),
            "--version" | "-V" => Ok(CommandKind::Version),
            _ => Err(()),
        }
    }
}

// Not actually used, but serves as a guarantee that every command has a
// corresponding CommandKind.
impl From<Command> for CommandKind {
    fn from(command: Command) -> Self {
        match command {
            Command::Node(_) => CommandKind::Node,
            Command::Compile(_) => CommandKind::Compile,
        }
    }
}

#[derive(clap::Args)]
pub struct NodeArgs {
    #[arg(
        long,
        value_name = "DIR",
        value_hint = clap::ValueHint::DirPath,
        long_help = "Directory where the node should store its data",
        env = "PATHFINDER_DATA_DIRECTORY",
        default_value_os_t = (&std::path::Component::CurDir).into()
    )]
    data_directory: PathBuf,

    #[arg(
        long = "ethereum.password",
        long_help = "The optional password to use for the Ethereum API",
        value_name = None,
        env = "PATHFINDER_ETHEREUM_API_PASSWORD",
    )]
    ethereum_password: Option<String>,

    #[arg(
        long = "ethereum.url",
        long_help = r"This should point to the WS RPC endpoint of your Ethereum entry-point, typically a local Ethereum client or a hosted gateway service such as Infura, Alchemy or Cloudflare.

Examples:
    alchemy: wss://eth-mainnet.g.alchemy.com/v2/<PROJECT_ID>
    geth:    wss://localhost:8545",
        value_name = "HTTP(s) URL",
        value_hint = clap::ValueHint::Url,
        env = "PATHFINDER_ETHEREUM_API_URL",
    )]
    ethereum_url: Url,

    #[arg(
        long = "http-rpc",
        long_help = "HTTP-RPC listening address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:9545",
        env = "PATHFINDER_HTTP_RPC_ADDRESS"
    )]
    rpc_address: SocketAddr,

    #[arg(
        long = "rpc.cors-domains",
        long_help = r"Comma separated list of domains from which Cross-Origin requests will be accepted by the RPC server.

Use '*' to indicate any domain and an empty list to disable CORS.

Examples:
    single: http://one.io
    a list: http://first.com,http://second.com:1234
    any:    *",
        value_name = "DOMAIN LIST",
        value_delimiter = ',',
        env = "PATHFINDER_RPC_CORS_DOMAINS"
    )]
    rpc_cors_domains: Vec<String>,

    #[arg(
        long = "rpc.root-version",
        long_help = "Version of the JSON-RPC API to serve on the / (root) path",
        default_value = "v08",
        env = "PATHFINDER_RPC_ROOT_VERSION"
    )]
    rpc_root_version: RootRpcVersion,

    #[arg(
        long = "rpc.execution-concurrency",
        long_help = "The number of Cairo VM executors that can work concurrently. Defaults to the \
                     number of CPU cores available.",
        env = "PATHFINDER_RPC_EXECUTION_CONCURRENCY"
    )]
    execution_concurrency: Option<NonZeroU32>,

    #[arg(
        long = "monitor-address",
        long_help = "The address at which pathfinder will serve monitoring related information",
        value_name = "IP:PORT",
        env = "PATHFINDER_MONITOR_ADDRESS"
    )]
    monitor_address: Option<SocketAddr>,

    #[clap(flatten)]
    network: NetworkCli,

    #[arg(
        long = "sqlite-wal",
        long_help = "Enable SQLite write-ahead logging",
        action = clap::ArgAction::Set,
        default_value = "true",
        env = "PATHFINDER_SQLITE_WAL",
    )]
    sqlite_wal: bool,

    #[arg(
        long = "max-rpc-connections",
        long_help = "Set the maximum number of connections allowed",
        env = "PATHFINDER_MAX_RPC_CONNECTIONS",
        default_value = "1024"
    )]
    max_rpc_connections: std::num::NonZeroUsize,

    #[arg(
        long = "sync.poll-interval",
        long_help = "New block poll interval in seconds (can use fractions)",
        default_value = "1",
        value_parser = parse_fractional_seconds,
        env = "PATHFINDER_HEAD_POLL_INTERVAL_SECONDS"
    )]
    poll_interval: Duration,

    #[arg(
        long = "sync.l1-poll-interval",
        long_help = "L1 state poll interval in seconds",
        default_value = "120",
        env = "PATHFINDER_L1_POLL_INTERVAL_SECONDS"
    )]
    l1_poll_interval: std::num::NonZeroU64,

    #[arg(
        long = "color",
        long_help = "This flag controls when to use colors in the output logs.",
        default_value = "auto",
        env = "PATHFINDER_COLOR",
        value_name = "WHEN"
    )]
    color: Color,

    #[arg(
        long = "log-output-json",
        long_help = "Enable JSON structured logging output.",
        default_value = "false",
        env = "PATHFINDER_LOG_OUTPUT_JSON",
        value_name = "BOOL"
    )]
    log_output_json: bool,

    #[arg(
        long = "disable-version-update-check",
        long_help = "Disable the periodic version update check.",
        default_value = "false",
        env = "PATHFINDER_DISABLE_VERSION_UPDATE_CHECK",
        value_name = "BOOL"
    )]
    disable_version_update_check: bool,

    #[cfg(feature = "p2p")]
    #[clap(flatten)]
    p2p_sync: P2PSyncCli,

    #[cfg(not(feature = "p2p"))]
    #[clap(skip)]
    p2p_sync: (),

    #[cfg(feature = "p2p")]
    #[clap(flatten)]
    p2p_consensus: P2PConsensusCli,

    #[cfg(not(feature = "p2p"))]
    #[clap(skip)]
    p2p_consensus: (),

    #[cfg(feature = "p2p")]
    #[clap(flatten)]
    consensus: ConsensusCli,

    #[cfg(feature = "p2p")]
    #[clap(flatten)]
    p2p_preconfirmed: P2PPreconfirmedCli,

    #[cfg(not(feature = "p2p"))]
    #[clap(skip)]
    p2p_preconfirmed: (),

    #[cfg(not(feature = "p2p"))]
    #[clap(skip)]
    consensus: (),

    #[cfg(feature = "p2p")]
    #[clap(flatten)]
    debug: DebugCli,

    #[cfg(not(feature = "p2p"))]
    #[clap(skip)]
    debug: (),

    #[cfg(feature = "cairo-native")]
    #[clap(flatten)]
    native_execution: NativeExecutionCli,

    #[cfg(not(feature = "cairo-native"))]
    #[clap(skip)]
    native_execution: (),

    #[clap(flatten)]
    websocket: WebsocketConfig,

    #[arg(
        long = "sync.verify_tree_node_data",
        long_help = r"When enabled, state tree node hashes are verified when loaded from disk.

This can be used to identify tree node data corruption which is useful when debugging a state commitment mismatch.

This should only be enabled for debugging purposes as it adds substantial processing cost to every block.
",
        default_value = "false",
        env = "PATHFINDER_VERIFY_TREE_NODE_HASHES",
        value_name = "BOOL"
    )]
    verify_tree_node_data: bool,

    #[arg(
        long = "rpc.batch-concurrency-limit",
        long_help = "Sets the concurrency limit for request batch processing. May lower the \
                     latency for large batches. ⚠ While the response order is eventually \
                     preserved, execution may be performed out of order.Setting this to 1 \
                     effectively disables concurrency.",
        env = "PATHFINDER_RPC_BATCH_CONCURRENCY_LIMIT",
        default_value = "1"
    )]
    rpc_batch_concurrency_limit: NonZeroUsize,

    #[arg(
        long = "rpc.disable-batch-requests",
        long_help = "Disable serving batch requests to JSON-RPC API.",
        default_value = "false",
        env = "PATHFINDER_RPC_DISABLE_BATCH_REQUESTS",
        value_name = "BOOL"
    )]
    disable_batch_requests: bool,

    #[arg(
        long = "sync.enable",
        long_help = "Enable syncing the chain",
        env = "PATHFINDER_SYNC_ENABLED",
        default_value = "true",
        action=ArgAction::Set
    )]
    is_sync_enabled: bool,

    #[arg(
        long = "rpc.enable",
        long_help = "Enable serving RPC API",
        env = "PATHFINDER_RPC_ENABLED",
        default_value = "true",
        action=ArgAction::Set
    )]
    is_rpc_enabled: bool,

    #[arg(
        long = "gateway-api-key",
        value_name = "API_KEY",
        long_help = "Specify an API key for both the Starknet feeder gateway and gateway.",
        env = "PATHFINDER_GATEWAY_API_KEY"
    )]
    gateway_api_key: Option<String>,

    #[arg(
        long = "gateway.request-timeout",
        value_name = "Seconds",
        long_help = "Timeout duration for all gateway and feeder-gateway requests",
        env = "PATHFINDER_GATEWAY_REQUEST_TIMEOUT",
        default_value = "10"
    )]
    gateway_timeout: std::num::NonZeroU64,

    #[arg(
        long = "gateway.fetch-concurrency",
        long_help = "How many concurrent requests to send to the feeder gateway when fetching \
                     block data",
        env = "PATHFINDER_GATEWAY_FETCH_CONCURRENCY",
        default_value = "8"
    )]
    feeder_gateway_fetch_concurrency: std::num::NonZeroUsize,

    #[arg(
        long = "gateway.check-for-dns-updates-interval",
        value_name = "Seconds",
        long_help = "Interval for checking DNS updates for the gateway and feeder gateway URLs",
        env = "PATHFINDER_GATEWAY_CHECK_FOR_DNS_UPDATES_INTERVAL",
        default_value = "60"
    )]
    gateway_dns_refresh_interval: std::num::NonZeroU64,

    #[arg(
        long = "storage.event-filter-cache-size",
        long_help = format!(
            "The number of aggregate event bloom filters to cache in memory. Each filter covers a {} block range.
            This cache speeds up event related RPC queries at the cost of using extra memory.
            Each cached filter takes 16 MiB of memory.",
            pathfinder_storage::AGGREGATE_BLOOM_BLOCK_RANGE_LEN
        ),
        env = "PATHFINDER_STORAGE_EVENT_FILTER_CACHE_SIZE",
        default_value = "64"
    )]
    event_filter_cache_size: std::num::NonZeroUsize,

    #[arg(
        long = "submission-tracker-time-limit",
        long_help = "Duration for which submitted transactions are locally remembered as \
                     RECEIVED, in seconds",
        default_value = "300",
        env = "PATHFINDER_SUBMISSION_TRACKER_TIME_LIMIT"
    )]
    submission_tracker_time_limit: std::num::NonZeroU64,

    #[arg(
        long = "submission-tracker-size-limit",
        long_help = "Maximum number of transactions that are locally remembered as RECEIVED.",
        default_value = "30000",
        env = "PATHFINDER_SUBMISSION_TRACKER_SIZE_LIMIT"
    )]
    submission_tracker_size_limit: std::num::NonZeroUsize,

    #[arg(
        long = "rpc.get-events-event-filter-block-range-limit",
        long_help = format!(
            "The maximum number of blocks to be covered by aggregate Bloom filters when querying for events. Each filter covers a {} block range.
            This limit is used to prevent queries from taking too long.",
            pathfinder_storage::AGGREGATE_BLOOM_BLOCK_RANGE_LEN
        ),
        env = "PATHFINDER_RPC_GET_EVENTS_EVENT_FILTER_BLOCK_RANGE_LIMIT",
        default_value = format!("{}", 10 * pathfinder_storage::AGGREGATE_BLOOM_BLOCK_RANGE_LEN)
    )]
    get_events_event_filter_block_range_limit: std::num::NonZeroUsize,

    #[arg(
        long = "storage.blockchain-history",
        long_help = "When set to `archive` all historical blockchain data is preserved. When set to an integer N, only the last N+1 blocks of the blockchain are kept in the database. \
            This can be used to reduce the disk space usage at the cost of only being able to provide information for the latest N+1 blocks (the state for the latest block is always stored). \
            Defaults to `archive` if not specified.",
        env = "PATHFINDER_STORAGE_BLOCKCHAIN_HISTORY",
        value_name = "archive | N",
        value_parser = parse_blockchain_history
    )]
    blockchain_history: Option<BlockchainHistory>,

    #[arg(
        long = "storage.state-tries",
        long_help = "When set to `archive` all historical Merkle trie state is preserved. When set to an integer N, only the last N+1 states of the Merkle tries are kept in the database. \
            This can be used to reduce the disk space usage at the cost of only being able to provide storage proofs for the latest N+1 blocks (the state for the latest block is always stored). \
            Defaults to 20 if the database was created with pruning enabled or `archive` if the database is archive-mode.",
        env = "PATHFINDER_STORAGE_STATE_TRIES",
        value_name = "archive | N",
        value_parser = parse_state_tries
    )]
    state_tries: Option<StateTries>,

    #[arg(
        long = "rpc.custom-versioned-constants-json-path",
        long_help = "Path to a JSON file referencing sequencer versioned constants. The file maps \
                     Starknet version keys to paths of files containing the versioned constants, \
                     which are then used for blocks with that version _and_ subsequent versions \
                     smaller than a hard-coded versioned constants change (or the next key in the \
                     custom map). The paths in the custom map file can be relative to that file. \
                     Alternatively, for backwards compatibility, path to the version constants \
                     file can be passed directly, in which case it's used for the latest version.",
        env = "PATHFINDER_RPC_CUSTOM_VERSIONED_CONSTANTS_JSON_PATH"
    )]
    custom_versioned_constants_path: Option<PathBuf>,

    #[arg(
        long = "compiler.max-memory-usage-mib",
        long_help = "Maximum memory usage for the compiler in MiB. 

Setting this value too low may cause compilation of large classes to fail.",
        env = "PATHFINDER_COMPILER_MAX_MEMORY_USAGE_MIB",
        default_value_t = pathfinder_compiler::ResourceLimits::RECOMMENDED_MEMORY_USAGE_LIMIT_MIB,
        value_parser = clap::value_parser!(u64).range(COMPILER_MEMORY_USAGE_ALLOWED_RANGE),
    )]
    compiler_max_memory_usage_mib: u64,

    #[arg(
        long = "compiler.max-cpu-time-secs",
        long_help = "Maximum CPU time for the compiler in seconds. 

Setting this value too low may cause compilation of large classes to fail.",
        env = "PATHFINDER_COMPILER_MAX_CPU_TIME_SECONDS",
        default_value_t = pathfinder_compiler::ResourceLimits::RECOMMENDED_CPU_TIME_LIMIT,
        value_parser = clap::value_parser!(u64).range(COMPILER_CPU_TIME_ALLOWED_RANGE),
    )]
    compiler_max_cpu_time_secs: u64,

    #[clap(flatten)]
    compile_config: CompileConfig,

    #[arg(
        long = "sync.fetch-casm-from-fgw",
        long_help = "Do not compile classes locally, instead fetch them from the feeder gateway",
        env = "PATHFINDER_SYNC_FETCH_CASM_FROM_FGW",
        default_value = "false",
        action=ArgAction::Set
    )]
    fetch_casm_from_fgw: bool,

    #[arg(
        long = "shutdown.grace-period",
        value_name = "Seconds",
        long_help = "Timeout duration for graceful shutdown after receiving a SIGINT or SIGTERM",
        env = "PATHFINDER_SHUTDOWN_GRACE_PERIOD",
        default_value = "10"
    )]
    shutdown_grace_period: std::num::NonZeroU64,

    #[arg(
        long = "rpc.fee-estimation-epsilon",
        value_name = "Percentage",
        long_help = "Acceptable overhead to add on top of consumed L2 gas (g) during fee estimation (`estimateFee` and `simulate` RPC methods). \
            Setting a lower value gives a more precise fee estimation (in terms of L2 gas) but runs a higher risk of having to resort to a binary \
            search if the initial L2 gas limit (`g  + (g * EPSILON/100)`) is insufficient.",
        env = "PATHFINDER_RPC_FEE_ESTIMATION_EPSILON",
        default_value = "10",
        value_parser = parse_fee_estimation_epsilon
    )]
    fee_estimation_epsilon: Percentage,

    #[arg(
        long = "rpc.block-trace-cache-size",
        long_help = "Number of block traces to cache in memory for RPC calls.",
        default_value = "128",
        env = "PATHFINDER_RPC_BLOCK_TRACE_CACHE_SIZE"
    )]
    rpc_block_trace_cache_size: std::num::NonZeroUsize,

    #[cfg_attr(
        all(
            feature = "consensus-integration-tests",
            feature = "p2p",
            debug_assertions
        ),
        clap(flatten)
    )]
    #[cfg_attr(
        not(all(
            feature = "consensus-integration-tests",
            feature = "p2p",
            debug_assertions
        )),
        clap(skip)
    )]
    integration_testing: integration_testing::IntegrationTestingCli,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq)]
pub enum Color {
    Auto,
    Never,
    Always,
}

impl Color {
    /// Returns true if color should be enabled, either because the setting is
    /// [Color::Always], or because it is [Color::Auto] and stdout is
    /// targeting a terminal.
    pub fn is_color_enabled(&self) -> bool {
        use std::io::IsTerminal;
        match self {
            Color::Auto => std::io::stdout().is_terminal(),
            Color::Never => false,
            Color::Always => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum BlockifierLibfuncs {
    #[default]
    Audited,
    All,
    Experimental,
}

fn parse_blockifier_libfuncs(s: &str) -> Result<BlockifierLibfuncs, String> {
    match s {
        "audited" => Ok(BlockifierLibfuncs::Audited),
        "all" => Ok(BlockifierLibfuncs::All),
        "experimental" => Ok(BlockifierLibfuncs::Experimental),
        _ => Err("Unknown blockifier libfunc list".to_string()),
    }
}

impl From<BlockifierLibfuncs> for pathfinder_compiler::BlockifierLibfuncs {
    fn from(val: BlockifierLibfuncs) -> Self {
        match val {
            BlockifierLibfuncs::Audited => pathfinder_compiler::BlockifierLibfuncs::Audited,
            BlockifierLibfuncs::All => pathfinder_compiler::BlockifierLibfuncs::All,
            BlockifierLibfuncs::Experimental => {
                pathfinder_compiler::BlockifierLibfuncs::Experimental
            }
        }
    }
}

#[derive(clap::Args, Clone)]
pub struct CompileConfig {
    #[arg(
        long = "blockifier.libfunc-list",
        long_help = "This names the libfunc list to be used in Starknet program validation.

The default is suitable for all uses except testing.",
        default_value = "audited",
        value_name = "audited | all | experimental",
        value_parser = parse_blockifier_libfuncs,
        env = "PATHFINDER_BLOCKIFIER_LIBFUNC_LIST"
    )]
    pub blockifier_libfuncs: BlockifierLibfuncs,

    #[arg(
        long = "max-bytecode-size",
        long_help = "Maximum allowed bytecode size for Starknet programs.",
        default_value = "81920",
        value_name = "bytes",
        env = "PATHFINDER_MAX_BYTECODE_SIZE"
    )]
    pub max_bytecode_size: usize,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq)]
pub enum RootRpcVersion {
    V06,
    V07,
    V08,
    V09,
    V10,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlockchainHistory {
    Prune(u64),
    Archive,
}

fn parse_blockchain_history(s: &str) -> Result<BlockchainHistory, String> {
    match s {
        "archive" => Ok(BlockchainHistory::Archive),
        _ => {
            let value: u64 = s
                .parse()
                .map_err(|_| "Expected either `archive` or a number".to_string())?;
            Ok(BlockchainHistory::Prune(value))
        }
    }
}

impl From<BlockchainHistory> for pathfinder_storage::pruning::BlockchainHistoryMode {
    fn from(val: BlockchainHistory) -> Self {
        match val {
            BlockchainHistory::Prune(num_blocks_kept) => {
                pathfinder_storage::pruning::BlockchainHistoryMode::Prune { num_blocks_kept }
            }
            BlockchainHistory::Archive => {
                pathfinder_storage::pruning::BlockchainHistoryMode::Archive
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StateTries {
    Pruned(u64),
    Archive,
}

fn parse_state_tries(s: &str) -> Result<StateTries, String> {
    match s {
        "archive" => Ok(StateTries::Archive),
        _ => {
            let value: u64 = s
                .parse()
                .map_err(|_| "Expected either `archive` or a number".to_string())?;
            Ok(StateTries::Pruned(value))
        }
    }
}

impl From<StateTries> for pathfinder_storage::TriePruneMode {
    fn from(val: StateTries) -> Self {
        match val {
            StateTries::Pruned(num_blocks_kept) => {
                pathfinder_storage::TriePruneMode::Prune { num_blocks_kept }
            }
            StateTries::Archive => pathfinder_storage::TriePruneMode::Archive,
        }
    }
}

fn parse_fee_estimation_epsilon(s: &str) -> Result<Percentage, String> {
    let value: u8 = s
        .parse()
        .map_err(|_| "Expected a number (u8)".to_string())
        .and_then(|value| {
            if value > 100 {
                Err("Expected a number between 0 and 100".to_string())
            } else {
                Ok(value)
            }
        })?;

    Ok(Percentage::new(value))
}

fn parse_fractional_seconds(s: &str) -> Result<Duration, String> {
    let seconds: f64 = s
        .parse()
        .map_err(|_| "Expected a number (f64)".to_string())?;
    let duration = Duration::try_from_secs_f64(seconds).map_err(|e| e.to_string())?;
    Ok(duration)
}

#[cfg(feature = "p2p")]
fn parse_felt(s: &str) -> Result<Felt, String> {
    let felt = Felt::from_hex_str(s).map_err(|e| ToString::to_string(&e))?;
    Ok(felt)
}

#[derive(clap::Args)]
struct NetworkCli {
    #[arg(
        long = "network",
        long_help = r"Specify the Starknet network for pathfinder to operate on.

Note that 'custom' requires also setting the --gateway-url and --feeder-gateway-url options.",
        value_enum,
        env = "PATHFINDER_NETWORK"
    )]
    network: Option<Network>,

    #[arg(
        long,
        long_help = "Set a custom Starknet chain ID (e.g. SN_SEPOLIA)",
        value_name = "CHAIN ID",
        env = "PATHFINDER_CHAIN_ID",
        required_if_eq("network", Network::Custom)
    )]
    chain_id: Option<String>,
    #[arg(
        long = "feeder-gateway-url",
        value_name = "URL",
        value_hint = clap::ValueHint::Url,
        long_help = "Specify a custom Starknet feeder gateway url. Can be used to run pathfinder on a custom Starknet network, or to use a gateway proxy. Requires '--network custom'.",
        env = "PATHFINDER_FEEDER_GATEWAY_URL",
        required_if_eq("network", Network::Custom),
    )]
    feeder_gateway: Option<Url>,

    #[arg(
        long = "gateway-url",
        value_name = "URL",
        value_hint = clap::ValueHint::Url,
        long_help = "Specify a custom Starknet gateway url. Can be used to run pathfinder on a custom Starknet network, or to use a gateway proxy. Requires '--network custom'.",
        env = "PATHFINDER_GATEWAY_URL",
        required_if_eq("network", Network::Custom),
    )]
    gateway: Option<Url>,

    #[arg(
        long = "gateway.compress-requests",
        long_help = "Compress requests sent to the Starknet gateway, if they contain nonempty proofs. Requests that do not contain a proof are not compressed. Requires '--network custom'.",
        action = clap::ArgAction::Set,
        default_value = "true",
    )]
    compress_gateway_requests: bool,
}

#[cfg(feature = "p2p")]
#[derive(clap::Args)]
struct DebugCli {
    #[arg(
        long = "debug.pretty-log",
        long_help = "Enable pretty logging, which is especially helpful when debugging p2p behavior",
        action = clap::ArgAction::Set,
        default_value = "false",
        env = "PATHFINDER_PRETTY_LOG",
    )]
    pretty_log: bool,

    #[arg(
        long = "debug.restart-delay",
        long_help = "L2 restart delay after failure, in seconds",
        action = clap::ArgAction::Set,
        default_value = "60",
        env = "PATHFINDER_RESTART_DELAY",
    )]
    restart_delay: u64,
}

#[cfg(feature = "cairo-native")]
#[derive(clap::Args)]
struct NativeExecutionCli {
    #[arg(
        long = "rpc.native-execution",
        long_help = "Enable Cairo native execution for RPC calls.",
        action = clap::ArgAction::Set,
        default_value = "false",
        env = "PATHFINDER_RPC_NATIVE_EXECUTION"
    )]
    is_native_execution_enabled: bool,

    #[arg(
        long = "rpc.native-execution-class-cache-size",
        long_help = "Number of Native classes to cache temporarily on disk.",
        action = clap::ArgAction::Set,
        default_value = "512",
        env = "PATHFINDER_RPC_NATIVE_EXECUTION_CLASS_CACHE_SIZE"
    )]
    class_cache_size: NonZeroUsize,

    #[arg(
        long = "rpc.native-execution-compiler-optimization-level",
        long_help = "Optimization level for the Cairo native compiler. Valid values are 0(none), 1 (less), 2 (default), and 3 (aggressive).",
        action = clap::ArgAction::Set,
        default_value = "2",
        env = "PATHFINDER_RPC_NATIVE_EXECUTION_COMPILER_OPTIMIZATION_LEVEL"
    )]
    optimization_level: u8,

    #[arg(
        long = "rpc.native-execution-force-use-for-incompatible-classes",
        long_help = "Force use of Cairo native execution even for Sierra classes before 1.7.0 that are known to result in incorrect cost calculation.",
        action = clap::ArgAction::Set,
        default_value = "false",
        env = "PATHFINDER_RPC_NATIVE_EXECUTION_FORCE_USE_FOR_INCOMPATIBLE_CLASSES"
    )]
    force_use_for_incompatible_classes: bool,
}

#[cfg(feature = "p2p")]
#[derive(clap::Args)]
struct ConsensusCli {
    #[arg(
        long = "consensus.enable",
        long_help = "Enable Starknet consensus node (validator).",
        action = clap::ArgAction::Set,
        default_value = "false",
        env = "PATHFINDER_CONSENSUS_ENABLE",
    )]
    is_consensus_enabled: bool,

    #[arg(
        long = "consensus.my-validator-address",
        long_help = "Address of this validator node.",
        value_name = "ADDRESS",
        value_parser = parse_felt,
        env = "PATHFINDER_CONSENSUS_MY_VALIDATOR_ADDRESS",
        required_if_eq("is_consensus_enabled", "true"),
    )]
    my_validator_address: Option<Felt>,

    #[arg(
        long = "consensus.validator-addresses",
        long_help = "Addresses of other validators, ie. excluding our own node.",
        value_name = "ADDRESS_LIST",
        value_parser = parse_felt,
        value_delimiter = ',',
        env = "PATHFINDER_CONSENSUS_VALIDATOR_ADDRESSES",
        required_if_eq("is_consensus_enabled", "true"),
    )]
    validator_addresses: Vec<Felt>,

    #[arg(
        long = "consensus.proposer-addresses",
        long_help = "Addresses of proposers. If provided, these will be used instead of fetching from L2.",
        value_name = "ADDRESS_LIST",
        value_parser = parse_felt,
        value_delimiter = ',',
        env = "PATHFINDER_CONSENSUS_PROPOSER_ADDRESSES",
    )]
    proposer_addresses: Vec<Felt>,

    #[arg(
        long = "consensus.history-depth",
        long_help = "How many historical consensus engines (ie. those prior to the current one) to keep enabled. Warning! Setting this value to below 2 may stall small networks in some circumstances.",
        action = clap::ArgAction::Set,
        default_value = "10",
        value_name = "DEPTH",
        value_parser = clap::value_parser!(u64).range(0..=10),
        env = "PATHFINDER_CONSENSUS_HISTORY_DEPTH",
    )]
    history_depth: u64,

    #[arg(
        long = "consensus.l1-gas-price-tolerance",
        value_name = "Percentage",
        long_help = "Maximum allowed tolerance for L1 gas price changes from the rolling average when validating new proposals.",
        action = clap::ArgAction::Set,
        default_value = "20",
        value_parser = clap::value_parser!(u8).range(0..=100),
        env = "PATHFINDER_CONSENSUS_L1_GAS_PRICE_TOLERANCE",
    )]
    l1_gas_price_tolerance: u8,

    #[arg(
        long = "consensus.l1-gas-price-max-time-gap",
        value_name = "Seconds",
        long_help = "Maximum allowed time gap between the requested timestamp and the latest L1 gas price sample when validating new proposals. If exceeded, the data is considered stale.",
        action = clap::ArgAction::Set,
        default_value = "600",
        value_parser = clap::value_parser!(u64),
        env = "PATHFINDER_CONSENSUS_L1_GAS_PRICE_MAX_TIME_GAP",
    )]
    l1_gas_price_max_time_gap: u64,
}

#[derive(clap::ValueEnum, Clone, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Network {
    Mainnet,
    SepoliaTestnet,
    SepoliaIntegration,
    Custom,
}

impl From<Network> for clap::builder::OsStr {
    fn from(value: Network) -> Self {
        match value {
            Network::Mainnet => "mainnet",
            Network::SepoliaTestnet => "sepolia-testnet",
            Network::SepoliaIntegration => "sepolia-integration",
            Network::Custom => "custom",
        }
        .into()
    }
}

fn parse_cors(inputs: Vec<String>) -> Result<Option<AllowedOrigins>, RpcCorsDomainsParseError> {
    if inputs.is_empty() {
        return Ok(None);
    }

    if inputs.len() == 1 && inputs[0] == "*" {
        return Ok(Some(AllowedOrigins::Any));
    }

    if inputs.iter().any(|s| s == "*") {
        return Err(RpcCorsDomainsParseError::WildcardAmongOtherValues);
    }

    let valid_origins = inputs
        .into_iter()
        .map(|input| match url::Url::parse(&input) {
            // Valid URL but has to be limited to origin form, i.e. no path, query, trailing slash
            // for default path etc.
            Ok(url) => {
                let origin = url.origin();

                if !origin.is_tuple() {
                    return Err(RpcCorsDomainsParseError::InvalidDomain(input));
                }

                if origin.ascii_serialization() == input {
                    Ok(input)
                } else {
                    // Valid URL but not a valid origin
                    Err(RpcCorsDomainsParseError::InvalidDomain(input))
                }
            }
            // Not an URL hence invalid origin
            Err(_e) => {
                eprintln!("Url_parse_error: {_e}");
                Err(RpcCorsDomainsParseError::InvalidDomain(input))
            }
        })
        .collect::<Result<HashSet<_>, RpcCorsDomainsParseError>>()?;

    Ok(Some(AllowedOrigins::List(
        valid_origins.into_iter().collect(),
    )))
}

pub fn parse_cors_or_exit(input: Vec<String>) -> Option<AllowedOrigins> {
    use clap::error::ErrorKind;

    match parse_cors(input) {
        Ok(parsed) => parsed,
        Err(error) => Cli::command()
            .error(ErrorKind::ValueValidation, error)
            .exit(),
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
enum RpcCorsDomainsParseError {
    #[error("Invalid allowed domain for CORS: {0}.")]
    InvalidDomain(String),
    #[error(
        "Specify either wildcard '*' or a comma separated list of allowed domains for CORS, not \
         both."
    )]
    WildcardAmongOtherValues,
}

fn parse_versioned_constants(
    path: &Path,
) -> Result<VersionedConstantsMap, ParseVersionedConstantsError> {
    let mut target = BTreeMap::new();
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let src_res: Result<HashMap<String, String>, _> = serde_json::from_reader(reader);
    if let Ok(source) = src_res {
        let dir_path = path.parent().ok_or_else(|| {
            ParseVersionedConstantsError::Io(std::io::Error::other(
                "Version constants map file path empty",
            ))
        })?;
        for (raw_version, rel_path) in source {
            let version = raw_version.parse::<StarknetVersion>().map_err(|_| {
                ParseVersionedConstantsError::ParseMap(serde::de::Error::custom(format!(
                    "Invalid Starknet version \"{raw_version}\""
                )))
            })?;
            let abs_path = std::fs::canonicalize(dir_path.join(rel_path))?;
            let constants = VersionedConstants::from_path(&abs_path)?;
            target.insert(version, Cow::Owned(constants));
        }
    } else {
        // logging isn't set up yet...
        eprintln!("Unknown versioned constants map file format - trying legacy...");
        let constants = VersionedConstants::from_path(path)?;
        target.insert(
            VersionedConstantsMap::latest_version(),
            Cow::Owned(constants),
        );
    }

    if target.is_empty() {
        return Err(ParseVersionedConstantsError::ParseMap(
            serde::de::Error::custom("Version constants map file specified but empty"),
        ));
    }

    Ok(VersionedConstantsMap::custom(target))
}

pub fn parse_versioned_constants_or_exit(path: &Path) -> VersionedConstantsMap {
    use clap::error::ErrorKind;

    match parse_versioned_constants(path) {
        Ok(versioned_constants) => versioned_constants,
        Err(error) => Cli::command()
            .error(ErrorKind::ValueValidation, error)
            .exit(),
    }
}

#[derive(Debug, thiserror::Error)]
enum ParseVersionedConstantsError {
    #[error("IO error while reading versioned constants: {0}.")]
    Io(#[from] std::io::Error),
    #[error("Parse error while loading versioned constants map: {0}.")]
    ParseMap(#[from] serde_json::Error),
    #[error("Parse error while loading versioned constants: {0}.")]
    Parse(#[from] pathfinder_executor::VersionedConstantsError),
}

pub struct Config {
    pub data_directory: PathBuf,
    pub ethereum: Ethereum,
    pub rpc_address: SocketAddr,
    pub rpc_cors_domains: Option<AllowedOrigins>,
    pub rpc_root_version: RootRpcVersion,
    pub websocket: WebsocketConfig,
    pub monitor_address: Option<SocketAddr>,
    pub network: Option<NetworkConfig>,
    pub execution_concurrency: Option<std::num::NonZeroU32>,
    pub sqlite_wal: JournalMode,
    pub max_rpc_connections: std::num::NonZeroUsize,
    pub poll_interval: Duration,
    pub l1_poll_interval: Duration,
    pub color: Color,
    pub log_output_json: bool,
    pub disable_version_update_check: bool,
    pub sync_p2p: P2PSyncConfig,
    pub consensus_p2p: P2PConsensusConfig,
    pub preconfirmed_p2p: P2PPreconfirmedConfig,
    pub debug: DebugConfig,
    pub verify_tree_hashes: bool,
    pub rpc_batch_concurrency_limit: NonZeroUsize,
    pub disable_batch_requests: bool,
    pub is_sync_enabled: bool,
    pub is_rpc_enabled: bool,
    pub gateway_api_key: Option<String>,
    pub gateway_timeout: Duration,
    pub gateway_dns_refresh_interval: Duration,
    pub event_filter_cache_size: NonZeroUsize,
    pub get_events_event_filter_block_range_limit: NonZeroUsize,
    pub blockchain_history: Option<BlockchainHistory>,
    pub state_tries: Option<StateTries>,
    pub versioned_constants_map: VersionedConstantsMap,
    pub compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    pub blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    pub feeder_gateway_fetch_concurrency: NonZeroUsize,
    pub fetch_casm_from_fgw: bool,
    pub shutdown_grace_period: Duration,
    pub fee_estimation_epsilon: Percentage,
    pub native_execution: NativeExecutionConfig,
    pub submission_tracker_time_limit: NonZeroU64,
    pub submission_tracker_size_limit: NonZeroUsize,
    pub rpc_block_trace_cache_size: NonZeroUsize,
    pub consensus: Option<ConsensusConfig>,
    /// Integration testing config, only available on debug builds with `p2p`
    /// and `consensus-integration-tests` features enabled.
    pub integration_testing: integration_testing::IntegrationTestingConfig,
}

pub struct Ethereum {
    pub url: Url,
    pub password: Option<String>,
}

#[derive(Clone)]
pub enum NetworkConfig {
    Mainnet,
    SepoliaTestnet,
    SepoliaIntegration,
    Custom {
        gateway: Box<Url>,
        feeder_gateway: Box<Url>,
        chain_id: String,
        compress_gateway_requests: bool,
    },
}

pub struct DebugConfig {
    pub pretty_log: bool,
    pub restart_delay: Duration,
}

#[cfg(feature = "cairo-native")]
#[derive(Clone)]
pub struct NativeExecutionConfig {
    enabled: bool,
    class_cache_size: NonZeroUsize,
    optimization_level: u8,
    force_use_for_incompatible_classes: bool,
}

#[cfg(not(feature = "cairo-native"))]
#[derive(Clone)]
pub struct NativeExecutionConfig;

#[cfg(feature = "p2p")]
#[derive(Clone)]
pub struct ConsensusConfig {
    pub my_starknet_version: StarknetVersion,
    /// The validator address of the current node.
    pub my_validator_address: ContractAddress,
    /// The validator addresses of all validators in the validator set.
    pub validator_addresses: Vec<ContractAddress>,
    /// The proposer addresses of all proposers in the proposer set.
    pub proposer_addresses: Vec<ContractAddress>,
    /// How many historical consensus engines (ie. those prior to the current
    /// one) to keep enabled.
    pub history_depth: u64,
    /// Maximum allowed tolerance for L1 gas price changes from the rolling
    /// average when validating new proposals.
    pub l1_gas_price_tolerance: f64,
    /// Maximum allowed time gap between the requested timestamp and the latest
    /// L1 gas price sample when validating new proposals. If exceeded, the data
    /// is considered stale.
    pub l1_gas_price_max_time_gap: u64,
}

#[cfg(not(feature = "p2p"))]
#[derive(Clone)]
pub struct ConsensusConfig;

impl NetworkConfig {
    fn from_components(args: NetworkCli) -> Option<Self> {
        use Network::*;
        let cfg = match (
            args.network,
            args.gateway,
            args.feeder_gateway,
            args.chain_id,
        ) {
            (None, None, None, None) => return None,
            (Some(Custom), Some(gateway), Some(feeder_gateway), Some(chain_id)) => {
                NetworkConfig::Custom {
                    gateway: Box::new(gateway),
                    feeder_gateway: Box::new(feeder_gateway),
                    chain_id,
                    compress_gateway_requests: args.compress_gateway_requests,
                }
            }
            (Some(Custom), _, _, _) => {
                unreachable!("`--network custom` requirements are handled by clap derive")
            }
            // Handle non-custom variants in an inner match so that the compiler will force
            // us to handle a new network variants explicitly. Otherwise we end up with a
            // catch-all arm that would swallow new variants silently.
            (Some(non_custom), None, None, None) => match non_custom {
                Mainnet => NetworkConfig::Mainnet,
                SepoliaTestnet => NetworkConfig::SepoliaTestnet,
                SepoliaIntegration => NetworkConfig::SepoliaIntegration,
                Custom => unreachable!("Network::Custom handled in outer arm already"),
            },
            // clap does not support disallowing args based on an enum value, so we have check for
            // `--network non-custom` + custom required args manually.
            _ => {
                use clap::error::ErrorKind;

                Cli::command()
                    .error(
                        ErrorKind::ArgumentConflict,
                        "--gateway-url, --feeder-gateway-url and --chain-id may only be used with \
                         --network custom",
                    )
                    .exit()
            }
        };

        Some(cfg)
    }
}

#[cfg(not(feature = "p2p"))]
impl DebugConfig {
    fn parse(_: ()) -> Self {
        Self {
            pretty_log: false,
            restart_delay: Duration::from_secs(5),
        }
    }
}

#[cfg(feature = "p2p")]
impl DebugConfig {
    fn parse(args: DebugCli) -> Self {
        Self {
            pretty_log: args.pretty_log,
            restart_delay: Duration::from_secs(args.restart_delay),
        }
    }
}

#[cfg(not(feature = "cairo-native"))]
impl NativeExecutionConfig {
    fn parse(_: ()) -> Self {
        Self
    }

    pub fn is_enabled(&self) -> bool {
        false
    }

    pub fn class_cache_size(&self) -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }

    pub fn optimization_level(&self) -> u8 {
        0
    }

    pub fn force_use_for_incompatible_classes(&self) -> bool {
        false
    }
}

#[cfg(feature = "cairo-native")]
impl NativeExecutionConfig {
    fn parse(args: NativeExecutionCli) -> Self {
        Self {
            enabled: args.is_native_execution_enabled,
            class_cache_size: args.class_cache_size,
            optimization_level: args.optimization_level,
            force_use_for_incompatible_classes: args.force_use_for_incompatible_classes,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn class_cache_size(&self) -> NonZeroUsize {
        self.class_cache_size
    }

    pub fn optimization_level(&self) -> u8 {
        self.optimization_level
    }

    pub fn force_use_for_incompatible_classes(&self) -> bool {
        self.force_use_for_incompatible_classes
    }
}

#[cfg(not(feature = "p2p"))]
impl ConsensusConfig {
    fn parse_or_exit(_: ()) -> Option<Self> {
        None
    }
}

#[cfg(feature = "p2p")]
impl ConsensusConfig {
    fn parse_or_exit(consensus_cli: ConsensusCli) -> Option<Self> {
        consensus_cli.is_consensus_enabled.then(|| {
            let my_validator_address = consensus_cli
                .my_validator_address
                .as_ref()
                .expect("Required if `is_consensus_enabled` is true");
            let unique_validator_addresses = std::iter::once(my_validator_address)
                .chain(consensus_cli.validator_addresses.iter())
                .collect::<HashSet<_>>();

            if unique_validator_addresses.len() < 3 {
                Cli::command()
                    .error(
                        clap::error::ErrorKind::ValueValidation,
                        "At least 3 unique validator addresses are required in \
                         '--consensus.validator-addresses' and '--consensus.my-validator-address' \
                         combined.",
                    )
                    .exit();
            }

            Self {
                // TODO: This is the only supported starknet version for consensus at the moment.
                // TBD how a node's starknet version should be configured.
                my_starknet_version: StarknetVersion::V_0_14_0,
                my_validator_address: ContractAddress(
                    consensus_cli
                        .my_validator_address
                        .expect("Required if `is_consensus_enabled` is true"),
                ),
                validator_addresses: consensus_cli
                    .validator_addresses
                    .into_iter()
                    .map(ContractAddress)
                    .collect(),
                proposer_addresses: consensus_cli
                    .proposer_addresses
                    .into_iter()
                    .map(ContractAddress)
                    .collect(),
                history_depth: consensus_cli.history_depth,
                l1_gas_price_tolerance: consensus_cli.l1_gas_price_tolerance as f64 / 100.0,
                l1_gas_price_max_time_gap: consensus_cli.l1_gas_price_max_time_gap,
            }
        })
    }
}

impl Config {
    #[cfg_attr(not(feature = "cairo-native"), allow(clippy::unit_arg))]
    pub fn parse(args: Box<NodeArgs>) -> Self {
        let network = NetworkConfig::from_components(args.network);

        Config {
            data_directory: args.data_directory,
            ethereum: Ethereum {
                password: args.ethereum_password,
                url: args.ethereum_url,
            },
            rpc_address: args.rpc_address,
            rpc_cors_domains: parse_cors_or_exit(args.rpc_cors_domains),
            rpc_root_version: args.rpc_root_version,
            websocket: args.websocket,
            monitor_address: args.monitor_address,
            network,
            execution_concurrency: args.execution_concurrency,
            sqlite_wal: match args.sqlite_wal {
                true => JournalMode::WAL,
                false => JournalMode::Rollback,
            },
            max_rpc_connections: args.max_rpc_connections,
            poll_interval: args.poll_interval,
            l1_poll_interval: Duration::from_secs(args.l1_poll_interval.get()),
            color: args.color,
            log_output_json: args.log_output_json,
            disable_version_update_check: args.disable_version_update_check,
            sync_p2p: P2PSyncConfig::parse_or_exit(args.p2p_sync),
            consensus_p2p: P2PConsensusConfig::parse_or_exit(args.p2p_consensus),
            preconfirmed_p2p: P2PPreconfirmedConfig::parse_or_exit(args.p2p_preconfirmed),
            debug: DebugConfig::parse(args.debug),
            verify_tree_hashes: args.verify_tree_node_data,
            rpc_batch_concurrency_limit: args.rpc_batch_concurrency_limit,
            disable_batch_requests: args.disable_batch_requests,
            is_sync_enabled: args.is_sync_enabled,
            is_rpc_enabled: args.is_rpc_enabled,
            gateway_api_key: args.gateway_api_key,
            event_filter_cache_size: args.event_filter_cache_size,
            get_events_event_filter_block_range_limit: args
                .get_events_event_filter_block_range_limit,
            gateway_timeout: Duration::from_secs(args.gateway_timeout.get()),
            gateway_dns_refresh_interval: Duration::from_secs(
                args.gateway_dns_refresh_interval.get(),
            ),
            feeder_gateway_fetch_concurrency: args.feeder_gateway_fetch_concurrency,
            blockchain_history: args.blockchain_history,
            state_tries: args.state_tries,
            versioned_constants_map: args
                .custom_versioned_constants_path
                .map(|path| parse_versioned_constants_or_exit(&path))
                .unwrap_or_default(),
            compiler_resource_limits: pathfinder_compiler::ResourceLimits::new(
                // Convert MiB to bytes for the general config.
                args.compiler_max_memory_usage_mib * 1024 * 1024,
                args.compiler_max_cpu_time_secs,
            ),
            blockifier_libfuncs: args.compile_config.blockifier_libfuncs.into(),
            fetch_casm_from_fgw: args.fetch_casm_from_fgw,
            shutdown_grace_period: Duration::from_secs(args.shutdown_grace_period.get()),
            fee_estimation_epsilon: args.fee_estimation_epsilon,
            #[cfg_attr(not(feature = "cairo-native"), allow(clippy::unit_arg))]
            native_execution: NativeExecutionConfig::parse(args.native_execution),
            submission_tracker_time_limit: args.submission_tracker_time_limit,
            submission_tracker_size_limit: args.submission_tracker_size_limit,
            rpc_block_trace_cache_size: args.rpc_block_trace_cache_size,
            consensus: ConsensusConfig::parse_or_exit(args.consensus),
            integration_testing: integration_testing::IntegrationTestingConfig::parse(
                args.integration_testing,
            ),
        }
    }
}

#[derive(clap::Args, Clone)]
pub struct WebsocketConfig {
    #[arg(
        long = "rpc.websocket.enabled",
        long_help = "Enable RPC WebSocket transport at the \"/ws\" path",
        default_value = "false",
        env = "PATHFINDER_WEBSOCKET_ENABLED"
    )]
    pub enabled: bool,
    #[arg(
        long = "rpc.websocket.max-history",
        long_help = "The maximum number of historical messages to send for each topic when a new client subscribes. If set to `unlimited`, all historical messages are sent. If set to a number N, only the last N messages are sent. Defaults to 1024 if not specified.",
        default_value = "1024",
        value_name = "unlimited | N",
        value_parser = parse_websocket_history,
        env = "PATHFINDER_WEBSOCKET_MAX_HISTORY"
    )]
    pub max_history: WebsocketHistory,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WebsocketHistory {
    Limited(u64),
    Unlimited,
}

fn parse_websocket_history(s: &str) -> Result<WebsocketHistory, String> {
    match s {
        "unlimited" => Ok(WebsocketHistory::Unlimited),
        _ => {
            let value: u64 = s
                .parse()
                .map_err(|_| "Expected either `unlimited` or a number".to_string())?;
            Ok(WebsocketHistory::Limited(value))
        }
    }
}

impl From<WebsocketHistory> for pathfinder_rpc::jsonrpc::websocket::WebsocketHistory {
    fn from(val: WebsocketHistory) -> Self {
        match val {
            WebsocketHistory::Limited(limit) => {
                pathfinder_rpc::jsonrpc::websocket::WebsocketHistory::Limited(limit)
            }
            WebsocketHistory::Unlimited => {
                pathfinder_rpc::jsonrpc::websocket::WebsocketHistory::Unlimited
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::{AllowedOrigins, RpcCorsDomainsParseError};
    use crate::config::{parse_cors, ParseVersionedConstantsError};

    #[test]
    fn parse_cors_domains() {
        let empty = String::new();
        let wildcard = "*".to_owned();
        let valid = "http://valid.com:1234".to_owned();
        let not_url = "not_url".to_string();
        let with_path = "http://a.com/path".to_string();
        let with_query = "http://a.com/?query=x".to_string();
        let with_trailing_slash = format!("{valid}/");

        [
            (
                vec![empty.clone()],
                RpcCorsDomainsParseError::InvalidDomain(empty.clone()),
            ),
            (
                vec![empty, wildcard.clone()],
                RpcCorsDomainsParseError::WildcardAmongOtherValues,
            ),
            (
                vec![valid.clone(), wildcard.clone()],
                RpcCorsDomainsParseError::WildcardAmongOtherValues,
            ),
            (
                vec![wildcard.clone(), wildcard.clone()],
                RpcCorsDomainsParseError::WildcardAmongOtherValues,
            ),
            (
                vec![valid.clone(), with_trailing_slash.clone()],
                RpcCorsDomainsParseError::InvalidDomain(with_trailing_slash),
            ),
            (
                vec![valid.clone(), not_url.clone()],
                RpcCorsDomainsParseError::InvalidDomain(not_url),
            ),
            (
                vec![valid.clone(), with_path.clone()],
                RpcCorsDomainsParseError::InvalidDomain(with_path),
            ),
            (
                vec![valid.clone(), with_query.clone()],
                RpcCorsDomainsParseError::InvalidDomain(with_query),
            ),
        ]
        .into_iter()
        .for_each(|(input, expected_error)| {
            assert_eq!(
                parse_cors(input.clone()).unwrap_err(),
                expected_error,
                "input: {input:?}"
            );
        });

        [
            (vec![], None),
            (vec![wildcard], Some(AllowedOrigins::Any)),
            (
                vec![valid.clone()],
                Some(AllowedOrigins::List(vec![valid.clone()])),
            ),
            (
                vec![valid.clone(), valid.clone()],
                Some(AllowedOrigins::List(vec![valid])),
            ),
        ]
        .into_iter()
        .for_each(|(input, expected_ok)| {
            assert_eq!(
                parse_cors(input.clone()).unwrap(),
                expected_ok,
                "input: {input:?}"
            )
        });
    }

    #[test]
    fn parse_versioned_constants_fails_if_file_not_found() {
        assert_matches!(
            super::parse_versioned_constants("./nonexistent_versioned_constants.json".as_ref()).unwrap_err(),
            ParseVersionedConstantsError::Io(err) => assert_eq!(err.kind(), std::io::ErrorKind::NotFound)
        );
    }

    #[test]
    fn parse_versioned_constants_fails_on_parse_error() {
        assert_matches!(
            super::parse_versioned_constants("fixtures/invalid_versioned_constants.json".as_ref())
                .unwrap_err(),
            ParseVersionedConstantsError::ParseMap(_)
        )
    }

    #[test]
    fn parse_versioned_constants_legacy() {
        super::parse_versioned_constants(
            "fixtures/blockifier_versioned_constants_0_13_1_1.json".as_ref(),
        )
        .unwrap();
    }

    #[test]
    fn parse_versioned_constants_success() {
        super::parse_versioned_constants("fixtures/multi_versioned_constants.json".as_ref())
            .unwrap();
    }
}
