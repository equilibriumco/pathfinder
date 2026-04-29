#![deny(rust_2018_idioms)]

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ::p2p::sync::client::peer_agnostic::Client as P2PSyncClient;
use anyhow::Context;
use config::BlockchainHistory;
use metrics_exporter_prometheus::PrometheusBuilder;
use pathfinder_common::class_definition::SerializedSierraDefinition;
use pathfinder_common::{BlockNumber, Chain, ChainId, EthereumChain};
use pathfinder_ethereum::EthereumClient;
use pathfinder_gas_price::{L1GasPriceConfig, L1GasPriceProvider};
#[cfg(feature = "p2p")]
use pathfinder_lib::consensus::ConsensusTaskHandles;
use pathfinder_lib::state::{sync_gas_prices, L1GasPriceSyncConfig, SyncContext};
use pathfinder_lib::ConsensusChannels;
#[cfg(feature = "p2p")]
use pathfinder_lib::{config, consensus, monitoring, p2p_network, state};
#[cfg(not(feature = "p2p"))]
use pathfinder_lib::{config, monitoring, p2p_network, state};
use pathfinder_rpc::context::{EthContractAddresses, WebsocketContext};
use pathfinder_rpc::{Notifications, SyncState};
use pathfinder_storage::Storage;
use starknet_gateway_client::GatewayApi;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinError;
use tracing::info;

use crate::config::{CompileConfig, NetworkConfig, StateTries};

mod http_client_refresh;
mod update;

// The Cairo VM allocates felts on the stack, so during execution it's making
// a huge number of allocations. We get roughly two times better execution
// performance by using jemalloc (compared to the Linux glibc allocator).
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    let cli = config::parse_cli();
    match cli.command {
        config::Command::Node(args) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .unwrap()
            .block_on(async move {
                node_main(args).await?;
                Ok(())
            }),
        config::Command::Compile(config) => compile_main(config),
    }
}

async fn node_main(args: Box<config::NodeArgs>) -> anyhow::Result<Storage> {
    if std::env::var_os("RUST_LOG").is_none() {
        // Disable all dependency logs by default.
        std::env::set_var("RUST_LOG", "pathfinder=info,error");
    }

    // Configure rustls crypto provider.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("rustls crypto provider setup should not fail");

    let config = config::Config::parse(args);

    setup_tracing(
        config.color,
        config.debug.pretty_log,
        config.log_output_json,
    );

    info!(
        // this is expected to be $(last_git_tag)-$(commits_since)-$(commit_hash)
        version = pathfinder_version::VERSION,
        "🏁 Starting node."
    );

    if !config.data_directory.exists() {
        std::fs::DirBuilder::new()
            .create(&config.data_directory)
            .context("Creating database directory")?;
    }
    std::env::set_var("SQLITE_TMPDIR", &config.data_directory);

    permission_check(&config.data_directory)?;

    let available_parallelism = std::thread::available_parallelism()?;

    rayon::ThreadPoolBuilder::new()
        .thread_name(|thread_index| format!("rayon-{thread_index}"))
        .num_threads(available_parallelism.get())
        .build_global()?;

    // A readiness flag which is used to indicate that pathfinder is ready via
    // monitoring.
    let readiness = Arc::new(AtomicBool::new(false));

    let sync_state = Arc::new(SyncState::default());

    let ethereum = EthereumContext::setup(config.ethereum.url.clone(), &config.ethereum.password)
        .await
        .context("Creating Ethereum context")?;

    // Use the default starknet network if none was configured.
    let network = match config.network {
        Some(ref network) => network.clone(),
        None => ethereum
            .default_network()
            .context("Using default Starknet network based on Ethereum configuration")?,
    };
    let network_label = match &network {
        NetworkConfig::Mainnet => "mainnet",
        NetworkConfig::SepoliaTestnet => "testnet-sepolia",
        NetworkConfig::SepoliaIntegration => "integration-sepolia",
        NetworkConfig::Custom { .. } => "custom",
    };

    let pathfinder_context = PathfinderContext::configure_and_proxy_check(
        network,
        &config.data_directory,
        config.gateway_api_key.clone(),
        config.gateway_timeout,
        config.gateway_dns_refresh_interval,
    )
    .await
    .context("Configuring pathfinder")?;

    verify_networks(pathfinder_context.network, ethereum.chain)?;

    let gateway_public_key = pathfinder_context
        .gateway
        .public_key()
        .await
        .context("Fetching Starknet gateway public key")?;

    // Setup and verify database

    let storage_manager =
        pathfinder_storage::StorageBuilder::file(pathfinder_context.database.clone())
            .journal_mode(config.sqlite_wal)
            .event_filter_cache_size(config.event_filter_cache_size.get())
            .trie_prune_mode(config.state_tries.map(StateTries::into))
            .blockchain_history_mode(config.blockchain_history.map(BlockchainHistory::into))
            .migrate()?;

    let sync_storage = storage_manager
        // 5 is enough for normal sync operations, and then `available_parallelism` for
        // the rayon thread pool workers to use.
        .create_pool(NonZeroU32::new(5 + available_parallelism.get() as u32).unwrap())
        .context(
            r"Creating database connection pool for sync.

Hint: This is usually caused by exceeding the file descriptor limit of your system.
      Try increasing the file limit to using `ulimit` or similar tooling.",
        )?;

    // Set the rpc file connection limit to a fraction of the RPC connections.
    // Having this be too large is counter productive as disk IO will then slow down
    // all queries.
    let max_rpc_connections: u32 = config
        .max_rpc_connections
        .get()
        .try_into()
        .expect("usize should cast to u32");
    let rpc_storage = std::cmp::max(10, max_rpc_connections / 8);
    let rpc_storage = NonZeroU32::new(rpc_storage).expect("A non-zero minimum is set");
    let rpc_storage = storage_manager.create_read_only_pool(rpc_storage).context(
        r"Creating database connection pool for RPC

Hint: This is usually caused by exceeding the file descriptor limit of your system.
      Try increasing the file limit to using `ulimit` or similar tooling.",
    )?;

    let execution_storage_pool_size = config.execution_concurrency.unwrap_or_else(|| {
        std::num::NonZeroU32::new(available_parallelism.get() as u32)
            .expect("The number of CPU cores should be non-zero")
    });
    let execution_storage = storage_manager
        .create_read_only_pool(execution_storage_pool_size)
        .context(
            r"Creating database connection pool for execution

Hint: This is usually caused by exceeding the file descriptor limit of your system.
      Try increasing the file limit to using `ulimit` or similar tooling.",
        )?;
    // 5 is enough for normal sync operations, and then `available_parallelism` for
    // the rayon thread pool workers to use.
    let p2p_storage = storage_manager
        .create_pool(NonZeroU32::new(5 + available_parallelism.get() as u32).unwrap())
        .context(
            r"Creating database connection pool for p2p

Hint: This is usually caused by exceeding the file descriptor limit of your system.
      Try increasing the file limit to using `ulimit` or similar tooling.",
        )?;
    // like p2p_storage
    let consensus_storage = storage_manager
        .create_pool(NonZeroU32::new(5 + available_parallelism.get() as u32).unwrap())
        .context(
            r"Creating database connection pool for consensus

Hint: This is usually caused by exceeding the file descriptor limit of your system.
      Try increasing the file limit to using `ulimit` or similar tooling.",
        )?;

    let shutdown_storage = storage_manager
        .create_pool(NonZeroU32::new(1).unwrap())
        .context(
            r"Creating database connection pool for graceful shutdown

Hint: This is usually caused by exceeding the file descriptor limit of your system.
      Try increasing the file limit to using `ulimit` or similar tooling.",
        )?;

    info!(location=?pathfinder_context.database, "Database migrated.");
    if !config.integration_testing.is_db_verification_disabled() {
        verify_database(
            &sync_storage,
            pathfinder_context.network,
            &pathfinder_context.gateway,
        )
        .await
        .context("Verifying database")?;
    }

    sync_storage
        .connection()
        .context("Creating database connection")?
        .transaction()
        .context(r"Creating database transaction")?
        .prune_tries()
        .context("Pruning tries on startup")?;

    // Register signal handlers here, because we want to be able to interrupt long
    // running migrations or trie pruning. No tasks are spawned before this point so
    // we don't worry about detachment.
    let mut term_signal = signal(SignalKind::terminate())?;
    let mut int_signal = signal(SignalKind::interrupt())?;

    let (tx_pending, rx_pending) = tokio::sync::watch::channel(Default::default());

    let rpc_config = pathfinder_rpc::context::RpcConfig {
        batch_concurrency_limit: config.rpc_batch_concurrency_limit,
        disable_batch_requests: config.disable_batch_requests,
        get_events_event_filter_block_range_limit: config.get_events_event_filter_block_range_limit,
        fee_estimation_epsilon: config.fee_estimation_epsilon,
        versioned_constants_map: config.versioned_constants_map.clone(),
        native_execution: config.native_execution.is_enabled(),
        native_class_cache_size: config.native_execution.class_cache_size(),
        native_compiler_optimization_level: config.native_execution.optimization_level(),
        native_execution_force_use_for_incompatible_classes: config
            .native_execution
            .force_use_for_incompatible_classes(),
        submission_tracker_time_limit: config.submission_tracker_time_limit,
        submission_tracker_size_limit: config.submission_tracker_size_limit,
        block_trace_cache_size: config.rpc_block_trace_cache_size,
        compiler_resource_limits: config.compiler_resource_limits,
        blockifier_libfuncs: config.blockifier_libfuncs,
    };

    let notifications = Notifications::default();
    let context = pathfinder_rpc::context::RpcContext::new(
        rpc_storage,
        execution_storage,
        sync_state.clone(),
        pathfinder_context.network_id,
        pathfinder_context.contract_addresses,
        pathfinder_context.gateway.clone(),
        rx_pending.clone(),
        notifications.clone(),
        ethereum.client.clone(),
        rpc_config,
    );

    let context = if config.websocket.enabled {
        context.with_websockets(WebsocketContext::new(config.websocket.max_history.into()))
    } else {
        context
    };
    let submitted_tx_tracker = context.submission_tracker.clone();

    // Spawn monitoring if configured.
    if let Some(address) = config.monitor_address {
        spawn_monitoring(
            network_label,
            address,
            readiness.clone(),
            sync_state.clone(),
            &config.data_directory,
        )
        .await
        .context("Starting monitoring task")?;
    }

    // From this point onwards, until the final select, we don't exit the process
    // even if some error is encountered or a signal is received as it would result
    // in tasks being detached and cancelled abruptly without a chance to clean
    // up. We need to wait for the final select where we can cancel all the tasks
    // and wait for them to finish. Only then can we exit the process and return an
    // error if some of the tasks failed or no error if we have received a signal.

    let (sync_p2p_handle, sync_p2p_client) = p2p_network::sync::start(
        pathfinder_context.network_id,
        p2p_storage,
        config.sync_p2p.clone(),
        config.data_directory.clone(),
    )
    .await;

    let chain_id = pathfinder_context.network_id;
    let (consensus_p2p_handle, consensus_p2p_client_and_event_rx) = p2p_network::consensus::start(
        chain_id,
        config.consensus_p2p.clone(),
        config.data_directory.clone(),
    )
    .await;

    let (preconfirmed_p2p_handle, _preconfirmed_p2p_client_and_event_rx) =
        if config.preconfirmed_p2p.enable {
            p2p_network::preconfirmed::start(
                chain_id,
                config.preconfirmed_p2p.clone(),
                config.data_directory.clone(),
            )
            .await
        } else {
            (tokio::task::spawn(futures::future::pending()), None)
        };

    let integration_testing_config = config.integration_testing;

    // Create L1 gas price provider and sync task if consensus is enabled
    let gas_price_provider = if integration_testing_config.is_gas_price_validation_disabled() {
        None
    } else if let Some(consensus_config) = &config.consensus {
        let provider = L1GasPriceProvider::new({
            #[cfg(feature = "p2p")]
            {
                L1GasPriceConfig {
                    tolerance: consensus_config.l1_gas_price_tolerance,
                    max_time_gap_seconds: consensus_config.l1_gas_price_max_time_gap,
                    ..Default::default()
                }
            }
            #[cfg(not(feature = "p2p"))]
            {
                let _ = consensus_config;
                L1GasPriceConfig::default()
            }
        });

        // Spawn the L1 gas price sync task
        let sync_provider = provider.clone();
        let ethereum_client = ethereum.client.clone();
        util::task::spawn(async move {
            if let Err(e) = sync_gas_prices(
                ethereum_client,
                sync_provider,
                L1GasPriceSyncConfig::default(),
            )
            .await
            {
                tracing::error!(error = %e, "L1 gas price sync task failed");
            }
        });

        Some(provider)
    } else {
        None
    };

    #[cfg(feature = "p2p")]
    let (
        consensus_p2p_event_processing_handle,
        consensus_engine_handle,
        consensus_channels,
        worker_pool,
    ) = {
        let handles = if let Some(consensus_config) = &config.consensus {
            let wal_directory = config.data_directory.join("consensus").join("wal");
            if !wal_directory.exists() {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .create(&wal_directory)
                    .context("Creating consensus wal directory")?;
            }

            if let Some((event_rx, client)) = consensus_p2p_client_and_event_rx {
                consensus::start(
                    consensus_config.clone(),
                    chain_id,
                    consensus_storage,
                    client,
                    event_rx,
                    wal_directory,
                    &config.data_directory,
                    gas_price_provider.clone(),
                    config.verify_tree_hashes,
                    config.compiler_resource_limits,
                    config.blockifier_libfuncs,
                    // Does nothing in production builds. Used for integration testing only.
                    integration_testing_config.inject_failure_config(),
                )
            } else {
                ConsensusTaskHandles::pending()
            }
        } else {
            ConsensusTaskHandles::pending()
        };
        (
            handles.consensus_p2p_event_processing_handle,
            handles.consensus_engine_handle,
            handles.consensus_channels,
            handles.worker_pool,
        )
    };

    #[cfg(not(feature = "p2p"))]
    let (consensus_p2p_event_processing_handle, consensus_engine_handle, consensus_channels) = {
        let _ = (
            consensus_storage,
            consensus_p2p_client_and_event_rx,
            gas_price_provider,
        );
        (
            tokio::task::spawn(std::future::pending::<anyhow::Result<()>>()),
            tokio::task::spawn(std::future::pending::<anyhow::Result<()>>()),
            None::<ConsensusChannels>,
        )
    };

    let context = if let Some(consensus_info_watch) = consensus_channels
        .as_ref()
        .map(|cc| cc.consensus_info_watch.clone())
    {
        context.with_consensus_info_watch(consensus_info_watch)
    } else {
        context
    };

    let default_version = match config.rpc_root_version {
        config::RootRpcVersion::V06 => pathfinder_rpc::RpcVersion::V06,
        config::RootRpcVersion::V07 => pathfinder_rpc::RpcVersion::V07,
        config::RootRpcVersion::V08 => pathfinder_rpc::RpcVersion::V08,
        config::RootRpcVersion::V09 => pathfinder_rpc::RpcVersion::V09,
        config::RootRpcVersion::V10 => pathfinder_rpc::RpcVersion::V10,
    };

    let rpc_server = pathfinder_rpc::RpcServer::new(config.rpc_address, context, default_version);
    let rpc_server = match config.rpc_cors_domains {
        Some(ref allowed_origins) => rpc_server.with_cors(allowed_origins.clone()),
        None => rpc_server,
    };

    let http_client_refresh_handle =
        util::task::spawn(http_client_refresh::refresh_http_client_periodically(
            pathfinder_context.gateway.clone(),
            pathfinder_context.gateway_dns_refresh_interval,
        ));

    // Nodes that ran v0.17.0–v0.19.x with pruning enabled may have
    // class_definitions rows where definition IS NULL that were never filled due
    // to a storage bug. This task re-downloads any such definitions on startup.
    util::task::spawn(state::repair::repair_missing_class_definitions(
        sync_storage.clone(),
        pathfinder_context.gateway.clone(),
        config.compiler_resource_limits,
        config.blockifier_libfuncs,
        config.fetch_casm_from_fgw,
    ));

    let sync_handle = if config.is_sync_enabled {
        start_sync(
            sync_storage,
            pathfinder_context,
            ethereum.client,
            sync_state.clone(),
            &config,
            submitted_tx_tracker,
            tx_pending,
            consensus_channels,
            notifications,
            gateway_public_key,
            sync_p2p_client,
        )
    } else {
        tokio::task::spawn(futures::future::pending())
    };

    let rpc_handle = if config.is_rpc_enabled {
        match rpc_server
            .with_max_connections(config.max_rpc_connections.get())
            .spawn(&config.data_directory)
            .await
        {
            Ok((rpc_handle, on)) => {
                info!(%on, "📡 RPC server started");
                rpc_handle
            }
            Err(error) => tokio::task::spawn(std::future::ready(Err(
                error.context("RPC server failed to start")
            ))),
        }
    } else {
        tokio::spawn(std::future::pending())
    };

    if !config.disable_version_update_check {
        util::task::spawn(update::poll_github_for_releases());
    }

    // We are now ready.
    readiness.store(true, std::sync::atomic::Ordering::Relaxed);

    // Monitor our critical spawned process tasks.
    let main_result = tokio::select! {
        result = sync_handle => handle_critical_task_result("Feeder gateway sync", result),
        result = rpc_handle => handle_critical_task_result("RPC", result),
        result = sync_p2p_handle => handle_critical_task_result("Sync P2P network and handlers", result),
        result = consensus_p2p_handle => handle_critical_task_result("Consensus P2P network", result),
        result = preconfirmed_p2p_handle => handle_critical_task_result("Preconfirmed P2P network", result),
        result = consensus_p2p_event_processing_handle => handle_critical_task_result("Consensus P2P event processing", result),
        result = consensus_engine_handle => handle_critical_task_result("Consensus engine", result),
        result = http_client_refresh_handle => handle_critical_task_result("HTTP client refresh", result),
        _ = term_signal.recv() => {
            tracing::info!("TERM signal received");
            Ok(())
        }
        _ = int_signal.recv() => {
            tracing::info!("INT signal received");
            Ok(())
        }
    };

    // If we get here either a signal was received or a task ended unexpectedly,
    // which means we need to cancel all the remaining tasks.
    tracing::info!("Shutdown started, waiting for tasks to finish...");
    util::task::tracker::close();
    // Force exit after a grace period
    match tokio::time::timeout(config.shutdown_grace_period, util::task::tracker::wait()).await {
        Ok(_) => {
            tracing::info!("Shutdown finished successfully")
        }
        Err(_) => {
            tracing::error!("Some tasks failed to finish in time, forcing exit");
        }
    }

    // Join all worker pool threads so that they don't panic when the `p2p_task` is
    // cancelled.
    #[cfg(feature = "p2p")]
    if let Some(worker_pool) = worker_pool {
        match Arc::try_unwrap(worker_pool) {
            Ok(pool) => pool.join(),
            Err(pool) => {
                tracing::error!(
                    "Failed to join worker pool, refcount is: {}",
                    Arc::strong_count(&pool)
                );
            }
        }
    }

    let jh = tokio::task::spawn_blocking(|| -> anyhow::Result<Storage> {
        shutdown_storage
            .connection()
            .context("Creating database connection for graceful shutdown")?
            .transaction()
            .context("Creating database transaction for graceful shutdown")?
            .store_in_memory_state()
            .context("Storing in-memory DB state on shutdown")?;

        Ok(shutdown_storage)
    });

    // Wait for the shutdown storage task to finish.
    let shutdown_storage = jh.await.context("Running shutdown storage task")??;

    // If a RO db connection pool remains after all RW connection pools have been
    // dropped, WAL & SHM files are never cleaned up. To avoid this, we make sure
    // that all RO pools and all but one RW pools are dropped when task tracker
    // finishes waiting, and then we drop the last RW pool.
    main_result.map(|_| shutdown_storage)
}

fn compile_main(config: CompileConfig) -> anyhow::Result<()> {
    use std::io::{Read, Write};

    const SIERRA_DEFINITION_SIZE_ESTIMATE: usize = 400 * 1024; // 400 KiB
    let mut sierra_bytes = Vec::with_capacity(SIERRA_DEFINITION_SIZE_ESTIMATE);
    std::io::stdin()
        .read_to_end(&mut sierra_bytes)
        .context("reading Sierra from stdin")?;
    let sierra_definition = SerializedSierraDefinition::from_vec(sierra_bytes);

    let casm = pathfinder_compiler::compile_sierra_to_casm_impl(
        &sierra_definition,
        config.blockifier_libfuncs.into(),
        config.max_bytecode_size,
    )
    .context("compiling Sierra to CASM")?;

    std::io::stdout()
        .write_all(casm.as_slice())
        .context("writing CASM to stdout")
}

#[cfg(feature = "tokio-console")]
fn setup_tracing(color: config::Color, pretty_log: bool, json_log: bool) {
    use tracing_subscriber::prelude::*;

    // EnvFilter isn't really a Filter, so this we need this ugly workaround for
    // filtering with it. See https://github.com/tokio-rs/tracing/issues/1868 for more details.
    let env_filter = Arc::new(tracing_subscriber::EnvFilter::from_default_env());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(color.is_color_enabled())
        .with_target(pretty_log);
    let filter =
        tracing_subscriber::filter::dynamic_filter_fn(move |m, c| env_filter.enabled(m, c.clone()));

    if json_log {
        tracing_subscriber::registry()
            .with(fmt_layer.json().flatten_event(true).with_filter(filter))
            .with(console_subscriber::spawn())
            .init();
    } else if pretty_log {
        tracing_subscriber::registry()
            .with(fmt_layer.pretty().with_filter(filter))
            .with(console_subscriber::spawn())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(fmt_layer.compact().with_filter(filter))
            .with(console_subscriber::spawn())
            .init();
    }
}

#[cfg(not(feature = "tokio-console"))]
fn setup_tracing(color: config::Color, pretty_log: bool, json_log: bool) {
    use time::macros::format_description;

    let time_fmt = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    let time_fmt = tracing_subscriber::fmt::time::UtcTime::new(time_fmt);

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(pretty_log)
        .with_timer(time_fmt)
        .with_ansi(color.is_color_enabled());

    if json_log {
        subscriber.json().flatten_event(true).init();
    } else if pretty_log {
        subscriber.pretty().init();
    } else {
        subscriber.compact().init();
    }
}

fn permission_check(base: &std::path::Path) -> Result<(), anyhow::Error> {
    tempfile::tempfile_in(base).with_context(|| {
        format!(
            "Failed to create a file in {}. Make sure the directory is writable by the user \
             running pathfinder.",
            base.display()
        )
    })?;

    // well, don't really know what else to check

    Ok(())
}

#[cfg(feature = "p2p")]
#[allow(clippy::too_many_arguments)]
fn start_sync(
    storage: Storage,
    pathfinder_context: PathfinderContext,
    ethereum_client: EthereumClient,
    sync_state: Arc<SyncState>,
    config: &config::Config,
    submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker,
    tx_pending: tokio::sync::watch::Sender<pathfinder_rpc::PendingData>,
    consensus_channels: Option<ConsensusChannels>,
    notifications: Notifications,
    gateway_public_key: pathfinder_common::PublicKey,
    p2p_client: Option<P2PSyncClient>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    if config.sync_p2p.proxy {
        start_feeder_gateway_sync(
            storage,
            pathfinder_context,
            ethereum_client,
            sync_state,
            config,
            submitted_tx_tracker,
            tx_pending,
            notifications,
            gateway_public_key,
        )
    } else if let Some(consensus_channels) = consensus_channels {
        start_consensus_aware_fgw_sync(
            storage,
            pathfinder_context,
            ethereum_client,
            sync_state,
            config,
            submitted_tx_tracker,
            tx_pending,
            notifications,
            gateway_public_key,
            consensus_channels,
        )
    } else {
        let p2p_client = p2p_client.expect("P2P client is expected with the p2p feature enabled");
        start_p2p_sync(
            storage,
            pathfinder_context,
            ethereum_client,
            p2p_client,
            gateway_public_key,
            config.sync_p2p.l1_checkpoint_override,
            config.verify_tree_hashes,
            config.compiler_resource_limits,
            config.blockifier_libfuncs,
        )
    }
}

#[cfg(not(feature = "p2p"))]
#[allow(clippy::too_many_arguments)]
fn start_sync(
    storage: Storage,
    pathfinder_context: PathfinderContext,
    ethereum_client: EthereumClient,
    sync_state: Arc<SyncState>,
    config: &config::Config,
    submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker,
    tx_pending: tokio::sync::watch::Sender<pathfinder_rpc::PendingData>,
    _consensus_channels: Option<ConsensusChannels>,
    notifications: Notifications,
    gateway_public_key: pathfinder_common::PublicKey,
    _p2p_client: Option<P2PSyncClient>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    start_feeder_gateway_sync(
        storage,
        pathfinder_context,
        ethereum_client,
        sync_state,
        config,
        submitted_tx_tracker,
        tx_pending,
        notifications,
        gateway_public_key,
    )
}

#[allow(clippy::too_many_arguments)]
fn start_feeder_gateway_sync(
    storage: Storage,
    pathfinder_context: PathfinderContext,
    ethereum_client: EthereumClient,
    sync_state: Arc<SyncState>,
    config: &config::Config,
    submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker,
    tx_pending: tokio::sync::watch::Sender<pathfinder_rpc::PendingData>,
    notifications: Notifications,
    gateway_public_key: pathfinder_common::PublicKey,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let sync_context = SyncContext {
        storage,
        ethereum: ethereum_client,
        chain: pathfinder_context.network,
        chain_id: pathfinder_context.network_id,
        core_address: pathfinder_context.contract_addresses.l1_contract_address,
        sequencer: pathfinder_context.gateway,
        state: sync_state.clone(),
        head_poll_interval: config.poll_interval,
        l1_poll_interval: config.l1_poll_interval,
        pending_data: tx_pending,
        submitted_tx_tracker,
        block_validation_mode: state::l2::BlockValidationMode::Strict,
        notifications,
        block_cache_size: 10_000,
        restart_delay: config.debug.restart_delay,
        verify_tree_hashes: config.verify_tree_hashes,
        sequencer_public_key: gateway_public_key,
        fetch_concurrency: config.feeder_gateway_fetch_concurrency,
        fetch_casm_from_fgw: config.fetch_casm_from_fgw,
        compiler_resource_limits: config.compiler_resource_limits,
        blockifier_libfuncs: config.blockifier_libfuncs,
    };

    util::task::spawn(state::sync(sync_context, state::l1::sync, state::l2::sync))
}

#[cfg(feature = "p2p")]
#[allow(clippy::too_many_arguments)]
fn start_consensus_aware_fgw_sync(
    storage: Storage,
    pathfinder_context: PathfinderContext,
    ethereum_client: EthereumClient,
    sync_state: Arc<SyncState>,
    config: &config::Config,
    submitted_tx_tracker: pathfinder_rpc::tracker::SubmittedTransactionTracker,
    tx_pending: tokio::sync::watch::Sender<pathfinder_rpc::PendingData>,
    notifications: Notifications,
    gateway_public_key: pathfinder_common::PublicKey,
    consensus_channels: ConsensusChannels,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let sync_context = SyncContext {
        storage,
        ethereum: ethereum_client,
        chain: pathfinder_context.network,
        chain_id: pathfinder_context.network_id,
        core_address: pathfinder_context.contract_addresses.l1_contract_address,
        sequencer: pathfinder_context.gateway,
        state: sync_state.clone(),
        head_poll_interval: config.poll_interval,
        l1_poll_interval: config.l1_poll_interval,
        pending_data: tx_pending,
        submitted_tx_tracker,
        block_validation_mode: state::l2::BlockValidationMode::Strict,
        notifications,
        block_cache_size: 10_000,
        restart_delay: config.debug.restart_delay,
        verify_tree_hashes: config.verify_tree_hashes,
        sequencer_public_key: gateway_public_key,
        compiler_resource_limits: config.compiler_resource_limits,
        blockifier_libfuncs: config.blockifier_libfuncs,
        fetch_concurrency: config.feeder_gateway_fetch_concurrency,
        fetch_casm_from_fgw: config.fetch_casm_from_fgw,
    };

    util::task::spawn(state::consensus_sync(
        sync_context,
        state::l1::sync,
        state::l2::consensus_sync,
        consensus_channels,
    ))
}

#[cfg(feature = "p2p")]
#[allow(clippy::too_many_arguments)]
fn start_p2p_sync(
    storage: Storage,
    pathfinder_context: PathfinderContext,
    ethereum_client: EthereumClient,
    p2p_client: P2PSyncClient,
    gateway_public_key: pathfinder_common::PublicKey,
    l1_checkpoint_override: Option<pathfinder_ethereum::EthereumStateUpdate>,
    verify_tree_hashes: bool,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    use pathfinder_block_hashes::BlockHashDb;

    let sync = pathfinder_lib::sync::Sync {
        storage,
        p2p: p2p_client,
        eth_client: ethereum_client,
        eth_address: pathfinder_context.contract_addresses.l1_contract_address,
        fgw_client: pathfinder_context.gateway,
        chain_id: pathfinder_context.network_id,
        public_key: gateway_public_key,
        l1_checkpoint_override,
        verify_tree_hashes,
        compiler_resource_limits,
        blockifier_libfuncs,
        block_hash_db: Some(BlockHashDb::new(pathfinder_context.network)),
    };
    util::task::spawn(sync.run())
}

/// Spawns the monitoring task at the given address.
async fn spawn_monitoring(
    network: &str,
    address: SocketAddr,
    readiness: Arc<AtomicBool>,
    sync_state: Arc<SyncState>,
    data_directory: &Path,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let prometheus_handle = PrometheusBuilder::new()
        .add_global_label("network", network)
        .install_recorder()
        .context("Creating Prometheus recorder")?;

    metrics::gauge!("pathfinder_build_info", "version" => pathfinder_version::VERSION).set(1.0);

    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            metrics::gauge!("process_start_time_seconds").set(duration.as_secs() as f64)
        }
        Err(err) => tracing::error!("Failed to read system time: {:?}", err),
    }

    let (_, handle) = monitoring::spawn_server(
        address,
        readiness,
        sync_state,
        prometheus_handle,
        data_directory,
    )
    .await?;
    Ok(handle)
}

/// Convenience bundle for an Ethereum transport and chain.
struct EthereumContext {
    client: EthereumClient,
    chain: EthereumChain,
}

impl EthereumContext {
    /// Configure an [EthereumContext]'s transport and read the chain ID using
    /// it.
    async fn setup(url: reqwest::Url, password: &Option<String>) -> anyhow::Result<Self> {
        // Require WebSocket URL - EthereumClient uses WebSocket for all operations
        if !matches!(url.scheme(), "ws" | "wss") {
            anyhow::bail!(
                "Ethereum URL must use WebSocket protocol (ws:// or wss://), got: {url}\n\nHint: \
                 Change your --ethereum.url from http(s):// to ws(s)://"
            );
        }

        let client = if let Some(password) = password.as_ref() {
            EthereumClient::with_password(url, password).context("Creating Ethereum client")?
        } else {
            EthereumClient::new(url).context("Creating Ethereum client")?
        };

        let chain = client.get_chain().await.context(
            r"Determining Ethereum chain.

Hint: Make sure the provided ethereum.url and ethereum.password are good.",
        )?;

        Ok(Self { client, chain })
    }

    /// Maps the Ethereum network to its default Starknet network:
    ///     Mainnet => Mainnet
    ///     Sepolia => Testnet/Sepolia
    fn default_network(&self) -> anyhow::Result<NetworkConfig> {
        match self.chain {
            EthereumChain::Mainnet => Ok(NetworkConfig::Mainnet),
            EthereumChain::Sepolia => Ok(NetworkConfig::SepoliaTestnet),
            EthereumChain::Other(id) => {
                anyhow::bail!(
                    r"Implicit Starknet networks are only available for Ethereum mainnet and Sepolia, but the provided Ethereum network has chain ID = {id}.

If you are trying to connect to a custom Starknet on another Ethereum network, please use '--network custom'"
                )
            }
        }
    }
}

struct PathfinderContext {
    network: Chain,
    network_id: ChainId,
    gateway: starknet_gateway_client::Client,
    gateway_dns_refresh_interval: std::time::Duration,
    database: PathBuf,
    contract_addresses: EthContractAddresses,
}

/// Used to hide private fn's for [PathfinderContext].
mod pathfinder_context {
    use std::path::Path;
    use std::time::Duration;

    use anyhow::Context;
    use pathfinder_common::{Chain, ChainId};
    use pathfinder_ethereum::core_addr;
    use pathfinder_rpc::context::EthContractAddresses;
    use reqwest::Url;
    use starknet_gateway_client::Client as GatewayClient;

    use super::PathfinderContext;
    use crate::config::NetworkConfig;

    impl PathfinderContext {
        pub async fn configure_and_proxy_check(
            cfg: NetworkConfig,
            data_directory: &Path,
            api_key: Option<String>,
            gateway_timeout: Duration,
            gateway_dns_refresh_interval: Duration,
        ) -> anyhow::Result<Self> {
            let context = match cfg {
                NetworkConfig::Mainnet => Self {
                    network: Chain::Mainnet,
                    network_id: ChainId::MAINNET,
                    gateway: GatewayClient::mainnet(gateway_timeout).with_api_key(api_key),
                    gateway_dns_refresh_interval,
                    database: data_directory.join("mainnet.sqlite"),
                    contract_addresses: EthContractAddresses::new_known(core_addr::MAINNET),
                },
                NetworkConfig::SepoliaTestnet => Self {
                    network: Chain::SepoliaTestnet,
                    network_id: ChainId::SEPOLIA_TESTNET,
                    gateway: GatewayClient::sepolia_testnet(gateway_timeout).with_api_key(api_key),
                    gateway_dns_refresh_interval,
                    database: data_directory.join("testnet-sepolia.sqlite"),
                    contract_addresses: EthContractAddresses::new_known(core_addr::SEPOLIA_TESTNET),
                },
                NetworkConfig::SepoliaIntegration => Self {
                    network: Chain::SepoliaIntegration,
                    network_id: ChainId::SEPOLIA_INTEGRATION,
                    gateway: GatewayClient::sepolia_integration(gateway_timeout)
                        .with_api_key(api_key),
                    gateway_dns_refresh_interval,
                    database: data_directory.join("integration-sepolia.sqlite"),
                    contract_addresses: EthContractAddresses::new_known(
                        core_addr::SEPOLIA_INTEGRATION,
                    ),
                },
                NetworkConfig::Custom {
                    gateway,
                    feeder_gateway,
                    chain_id,
                    compress_gateway_requests,
                } => Self::configure_custom(
                    *gateway,
                    *feeder_gateway,
                    chain_id,
                    data_directory,
                    api_key,
                    gateway_timeout,
                    gateway_dns_refresh_interval,
                    compress_gateway_requests,
                )
                .await
                .context("Configuring custom network")?,
            };

            Ok(context)
        }

        /// Creates a [PathfinderContext] for a custom network. Provides
        /// additional verification by checking for a proxy gateway by
        /// comparing against L1 starknet address against of
        /// the known networks.
        #[allow(clippy::too_many_arguments)]
        async fn configure_custom(
            gateway: Url,
            feeder: Url,
            chain_id: String,
            data_directory: &Path,
            api_key: Option<String>,
            gateway_timeout: Duration,
            gateway_dns_refresh_interval: Duration,
            compress_gateway_requests: bool,
        ) -> anyhow::Result<Self> {
            use pathfinder_crypto::Felt;
            use starknet_gateway_client::GatewayApi;

            let gateway = GatewayClient::with_urls(gateway, feeder, gateway_timeout)
                .context("Creating gateway client")?
                .with_api_key(api_key)
                .with_compress_gateway_requests(compress_gateway_requests);

            let network_id =
                ChainId(Felt::from_be_slice(chain_id.as_bytes()).context("Parsing chain ID")?);

            let reply_contract_addresses = gateway
                .eth_contract_addresses()
                .await
                .context("Downloading starknet L1 address from gateway for proxy check")?;
            let l1_core_address = reply_contract_addresses.starknet.0;
            let contract_addresses = EthContractAddresses::new_custom(
                l1_core_address,
                reply_contract_addresses.eth_l2_token_address,
                reply_contract_addresses.strk_l2_token_address,
            );

            // Check for proxies by comparing the core address against those of the known
            // networks.
            let network = match l1_core_address.as_bytes() {
                x if x == core_addr::MAINNET => Chain::Mainnet,
                x if x == core_addr::SEPOLIA_TESTNET => Chain::SepoliaTestnet,
                x if x == core_addr::SEPOLIA_INTEGRATION => Chain::SepoliaIntegration,
                _ => Chain::Custom,
            };

            if network != Chain::Custom {
                tracing::info!(%network, "Proxy gateway detected");
            }

            let context = Self {
                network,
                network_id,
                gateway,
                gateway_dns_refresh_interval,
                database: data_directory.join("custom.sqlite"),
                contract_addresses,
            };

            Ok(context)
        }
    }
}

/// Errors if there is a mismatch between the starknet and ethereum networks.
fn verify_networks(starknet: Chain, ethereum: EthereumChain) -> anyhow::Result<()> {
    if starknet != Chain::Custom {
        let expected = match starknet {
            Chain::Mainnet => EthereumChain::Mainnet,
            Chain::SepoliaTestnet | Chain::SepoliaIntegration => EthereumChain::Sepolia,
            Chain::Custom => unreachable!("Already checked against"),
        };

        anyhow::ensure!(
            ethereum == expected,
            "Incorrect Ethereum network detected. Found {ethereum:?} but expected {expected:?} \
             for {starknet} Starknet"
        );
    }

    Ok(())
}

async fn verify_database(
    storage: &Storage,
    network: Chain,
    gateway_client: &starknet_gateway_client::Client,
) -> anyhow::Result<()> {
    let storage = storage.clone();

    let mut conn = storage.connection().context("Create database connection")?;
    let tx = conn.transaction().context("Create database transaction")?;

    let db_genesis = tx
        .block_id(BlockNumber::GENESIS.into())
        .context("Fetching genesis hash from database")?
        .map(|x| x.1);

    if let Some(database_genesis) = db_genesis {
        use pathfinder_common::consts::{
            MAINNET_GENESIS_HASH,
            SEPOLIA_INTEGRATION_GENESIS_HASH,
            SEPOLIA_TESTNET_GENESIS_HASH,
        };

        let db_network = match database_genesis {
            MAINNET_GENESIS_HASH => Chain::Mainnet,
            SEPOLIA_TESTNET_GENESIS_HASH => Chain::SepoliaTestnet,
            SEPOLIA_INTEGRATION_GENESIS_HASH => Chain::SepoliaIntegration,
            _ => Chain::Custom,
        };

        match (network, db_network) {
            (Chain::Custom, _) => {
                // Verify against gateway.
                let (_, gateway_hash) = gateway_client
                    .block_header(BlockNumber::GENESIS.into())
                    .await
                    .context("Downloading genesis block from gateway for database verification")?;

                anyhow::ensure!(
                    database_genesis == gateway_hash,
                    "Database genesis block {database_genesis} does not match gateway \
                     {gateway_hash}."
                );
            }
            (network, db_network) => anyhow::ensure!(
                network == db_network,
                "Database ({db_network}) does not match the expected network ({network})"
            ),
        }
    }

    Ok(())
}

fn handle_critical_task_result(
    task_name: &str,
    task_result: Result<anyhow::Result<()>, JoinError>,
) -> anyhow::Result<()> {
    match task_result {
        Ok(task_result) => {
            tracing::error!(?task_result, "{} task ended unexpectedly", task_name);
            task_result
        }
        Err(error) if error.is_panic() => {
            tracing::error!(%error, %task_name, "Task panicked");
            Err(anyhow::anyhow!("{task_name} task panicked"))
        }
        // Cancelling all tracked tasks via [`util::task::tracker::close()`] does not cause join
        // errors on registered task handles, so this is unexpected and we should threat it as error
        Err(_) => {
            tracing::error!(%task_name, "Task was cancelled unexpectedly");
            Err(anyhow::anyhow!(
                "{task_name} task was cancelled unexpectedly"
            ))
        }
    }
}
