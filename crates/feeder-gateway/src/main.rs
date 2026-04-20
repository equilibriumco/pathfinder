/// Serve feeder gateway REST endpoints required for pathfinder to sync.
///
/// Usage:
/// `cargo run --release -p feeder-gateway ./testnet-sepolia.sqlite`
///
/// Then pathfinder can be run with the following arguments to use this tool as
/// a sync source:
///
/// ```text
/// cargo run --release -p pathfinder -- \
///     --network custom --chain-id SN_SEPOLIA \
///     --ethereum.url https://eth-sepolia.alchemyapi.io/v2/YOUR_API_KEY
///     --gateway-url http://localhost:8080/gateway \
///     --feeder-gateway-url http://localhost:8080/feeder_gateway \
///     --data-directory /tmp
/// ```
///
/// Optionally this tool can simulate reorgs. To have the tool return data so
/// that pathfinder reorgs from block 50 to 40 use the following command line:
/// `cargo run --release -p pathfinder --example feeder_gateway
/// ./testnet-sepolia.sqlite --reorg-at-block 50 --reorg-to-block 40`
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{ArgAction, Args, Parser};
use futures::future::BoxFuture;
use futures::FutureExt;
use pathfinder_block_commitments::{
    calculate_event_commitment,
    calculate_receipt_commitment,
    calculate_transaction_commitment,
};
use pathfinder_common::class_definition::{
    SerializedCasmDefinition,
    SerializedOpaqueClassDefinition,
};
use pathfinder_common::integration_testing::debug_create_port_marker_file;
use pathfinder_common::prelude::*;
use pathfinder_common::state_update::ContractClassUpdate;
use pathfinder_common::{BlockId, Chain};
use pathfinder_storage::Storage;
use primitive_types::H160;
use serde::{Deserialize, Serialize};
use starknet_gateway_types::reply::state_update::{
    DeclaredSierraClass,
    DeployedContract,
    MigratedCompiledClass,
    ReplacedClass,
    StorageDiff,
};
use starknet_gateway_types::reply::{GasPrices, Status};
use tokio::sync::watch::{Receiver, Sender};
use tracing::Instrument;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};
use warp::Filter;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[arg(long_help = "Database path.")]
    pub database_path: PathBuf,
    #[arg(
        long,
        long_help = "Port to listen on, 0 means random OS assigned value",
        default_value = "8080"
    )]
    pub port: u16,
    #[arg(
        long,
        long_help = "If set, the process will wait for the marker file to become available. \
                     If the marker file is not immediately available, the feeder gateway \
                     will keep retrying until a timeout occurs.",
        value_name = "MARKER_FILE_PATH",
        action=ArgAction::Set
    )]
    pub wait_for_marker_file: Option<PathBuf>,
    #[command(flatten)]
    pub reorg: ReorgCli,
}

#[derive(Debug, Clone, Args)]
struct ReorgCli {
    #[arg(long, long_help = "Reorg should happen after this block", value_parser = parse_block_number, requires = "reorg_to_block")]
    pub reorg_at_block: Option<BlockNumber>,
    #[arg(long, long_help = "Reorg should roll back state to this block", value_parser = parse_block_number, requires = "reorg_at_block")]
    pub reorg_to_block: Option<BlockNumber>,
}

fn parse_block_number(s: &str) -> Result<BlockNumber, String> {
    let n: u64 = s
        .parse()
        .map_err(|e| format!("Invalid block number '{s}': {e}"))?;
    BlockNumber::new(n).ok_or_else(|| format!("Invalid block number '{s}'"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let (storage_tx, storage_rx) = tokio::sync::watch::channel(None);

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    if cli.wait_for_marker_file.is_none() {
        let storage = pathfinder_storage::StorageBuilder::file(cli.database_path.clone())
            .readonly()?
            .create_read_only_pool(NonZeroU32::new(10).unwrap())?;

        let chain = {
            let mut connection = storage.connection()?;
            let tx = connection.transaction()?;
            get_chain(&tx)?
        };
        storage_tx.send(Some((storage, chain)))?;
    }

    tokio::select! {
        // We wait for a marker file, because in a test scenario where the FGw opens an existing DB before Pathfinder,
        // wal and shm files will be created. Sqlite3 engine in Pathfinder will attempt to perform recovery
        // using those files, which requires locking the DB and will fail with `SQLITE_BUSY` (DB locked)
        // because there already are open RO connections from the FGw's pool and recovery requires no locks,
        // even if they are RO.
        storage_err = wait_for_marker_file(
            cli.wait_for_marker_file.clone(),
            cli.database_path.clone(),
            storage_tx,
            Duration::from_millis(500),
            Duration::from_secs(30),
        ) => {
            Err(storage_err)
        },
        res = serve(cli, storage_rx) => {
            res
        }
    }
}

#[derive(Debug, Clone)]
struct ReorgConfig {
    pub reorg_at_block: BlockNumber,
    pub reorg_to_block: BlockNumber,
}

/// Waits for the marker file to become available and then attempts to create a
/// read-only connection pool to the database. The task **does not join** if the
/// marker file becomes available or the `enable` flag is `false`, it just
/// returns **a pending future**. The task only returns if an error is
/// encountered, including a timeout.
fn wait_for_marker_file(
    marker_file: Option<PathBuf>,
    db_file: PathBuf,
    storage_tx: Sender<Option<(Storage, Chain)>>,
    poll_interval: Duration,
    timeout: Duration,
) -> impl Future<Output = anyhow::Error> {
    let Some(marker_file) = marker_file else {
        return futures::future::pending().boxed();
    };

    let jh = tokio::task::spawn_blocking(move || {
        let stopwatch = std::time::Instant::now();
        loop {
            match std::fs::exists(marker_file.clone()) {
                Ok(true) => {
                    let Ok(storage_manager) =
                        pathfinder_storage::StorageBuilder::file(db_file.clone()).readonly()
                    else {
                        tracing::info!(
                            "Creating read only storage manager failed, database is not ready, \
                             retrying..."
                        );
                        std::thread::sleep(poll_interval);
                        continue;
                    };

                    let Ok(storage) =
                        storage_manager.create_read_only_pool(NonZeroU32::new(10).unwrap())
                    else {
                        tracing::info!(
                            "Creating read only connection pool failed, database is not ready, \
                             retrying..."
                        );
                        std::thread::sleep(poll_interval);
                        continue;
                    };

                    let chain = {
                        if !storage.is_migrated()? {
                            tracing::info!("Database not yet migrated, retrying...");
                            std::thread::sleep(poll_interval);
                            continue;
                        }

                        Chain::Custom
                    };
                    tracing::info!("Database is now available");
                    return anyhow::Ok((storage, chain));
                }
                Ok(false) => {
                    tracing::info!("Marker file not yet available, retrying...");
                    std::thread::sleep(poll_interval);
                }
                Err(_) => {
                    tracing::info!("Could not check if marker file exists, retrying...");
                    std::thread::sleep(poll_interval);
                }
            }

            if stopwatch.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timed out waiting for the DB to become available."
                ));
            }
        }
    });

    fn err(e: anyhow::Error) -> BoxFuture<'static, anyhow::Error> {
        std::future::ready(e).boxed()
    }

    let storage_tx = storage_tx.clone();
    jh.then(
        move |join_result| match join_result.context("Joining blocking task") {
            Ok(task_result) => match task_result {
                Ok(storage_n_chain) => {
                    match storage_tx
                        .send(Some(storage_n_chain))
                        .context("Sending storage instance")
                    {
                        Ok(_) => std::future::pending().boxed(),
                        Err(e) => err(e),
                    }
                }
                Err(e) => err(e),
            },
            Err(e) => err(e),
        },
    )
    .boxed()
}

async fn serve(cli: Cli, storage_rx: Receiver<Option<(Storage, Chain)>>) -> anyhow::Result<()> {
    let reorg_config = cli.reorg.reorg_at_block.and_then(|reorg_at_block| {
        cli.reorg.reorg_to_block.map(|reorg_to_block| ReorgConfig {
            reorg_at_block,
            reorg_to_block,
        })
    });
    let reorged = Arc::new(AtomicBool::new(false));

    fn maybe_chain(storage_rx: Receiver<Option<(Storage, Chain)>>) -> Option<Chain> {
        storage_rx.borrow().clone().map(|(_, chain)| chain)
    }

    fn maybe_storage(storage_rx: Receiver<Option<(Storage, Chain)>>) -> Option<Storage> {
        storage_rx.borrow().clone().map(|(storage, _)| storage)
    }

    fn storage_unavailable_response() -> warp::http::Response<Vec<u8>> {
        warp::http::Response::builder()
            .status(500)
            .body(r"Storage unavailable".as_bytes().to_owned())
            .unwrap()
    }

    #[derive(Debug)]
    struct StorageUnavailable;
    impl warp::reject::Reject for StorageUnavailable {}

    fn storage_unavailable_rejection() -> warp::reject::Rejection {
        warp::reject::custom(StorageUnavailable)
    }

    let storage_rx_clone = storage_rx.clone();
    let get_contract_addresses = warp::path("get_contract_addresses").map(move || {
        let chain = maybe_chain(storage_rx_clone.clone()).unwrap_or(Chain::Custom);
        let addresses = contract_addresses(chain).unwrap();
        let reply =
            serde_json::json!({"GpsStatementVerifier": addresses.gps, "Starknet": addresses.core});
        warp::reply::json(&reply)
    });

    #[derive(Debug, Deserialize)]
    struct BlockIdParam {
        #[serde(default, rename = "blockNumber")]
        block_number: Option<String>,
        #[serde(default, rename = "blockHash")]
        block_hash: Option<BlockHash>,
        #[serde(default, rename = "includeBlock")]
        include_block: Option<bool>,
        #[serde(default, rename = "includeSignature")]
        include_signature: Option<bool>,
        #[serde(default, rename = "headerOnly")]
        header_only: Option<bool>,
    }

    impl TryInto<BlockId> for BlockIdParam {
        type Error = ();

        fn try_into(self) -> Result<BlockId, Self::Error> {
            if let Some(n) = self.block_number {
                if n == "latest" {
                    return Ok(BlockId::Latest);
                } else {
                    let n: u64 = n.parse().map_err(|_| ())?;
                    return Ok(BlockId::Number(BlockNumber::new_or_panic(n)));
                }
            }

            if let Some(h) = self.block_hash {
                return Ok(BlockId::Hash(h));
            }
            Err(())
        }
    }

    let get_block = warp::path("get_block")
        .and(warp::query::<BlockIdParam>())
        .and_then({
            let storage_rx = storage_rx.clone();
            let reorg_config = reorg_config.clone();
            let reorged = reorged.clone();

            move |block_id: BlockIdParam| {
                let storage_rx = storage_rx.clone();
                let reorg_config = reorg_config.clone();
                let reorged = reorged.clone();

                async move {
                    let header_only = block_id.header_only.unwrap_or(false);

                    match block_id.try_into() {
                        Ok(block_id) => {
                            let Some(storage) = maybe_storage(storage_rx) else {
                                return Err(storage_unavailable_rejection());
                            };

                            let block = tokio::task::spawn_blocking(move || {
                                let mut connection = storage.connection().unwrap();
                                let tx = connection.transaction().unwrap();

                                resolve_block(&tx, block_id, &reorg_config, reorged)
                            }).await.context("Joining blocking task").and_then(|res| res);

                            match block {
                                Ok(block) => {
                                    if header_only {
                                        #[derive(Serialize)]
                                        struct HashAndNumber {
                                            block_hash: BlockHash,
                                            block_number: BlockNumber,
                                        }

                                        let reply = HashAndNumber {
                                            block_hash: block.block_hash,
                                            block_number: block.block_number,
                                        };

                                        Ok(warp::reply::with_status(warp::reply::json(&reply), warp::http::StatusCode::OK))
                                    } else {
                                        Ok(warp::reply::with_status(warp::reply::json(&block), warp::http::StatusCode::OK))
                                    }
                                },
                                Err(e) => {
                                    tracing::error!("Error fetching block: {:?}", e);
                                    let error = serde_json::json!({"code": "StarknetErrorCode.BLOCK_NOT_FOUND", "message": "Block number not found"});
                                    Ok(warp::reply::with_status(warp::reply::json(&error), warp::http::StatusCode::BAD_REQUEST))
                                }
                            }
                        },
                        Err(_) => Err(warp::reject::reject()),
                    }
                }
            }
        });

    let get_signature = warp::path("get_signature")
        .and(warp::query::<BlockIdParam>())
        .and_then({
            let storage_rx = storage_rx.clone();
            move |block_id: BlockIdParam| {
                let storage_rx = storage_rx.clone();
                async move {
                    match block_id.try_into() {
                        Ok(block_id) => {
                            let Some(storage) = maybe_storage(storage_rx) else {
                                return Err(storage_unavailable_rejection());
                            };

                            let signature = tokio::task::spawn_blocking(move || {
                                let mut connection = storage.connection().unwrap();
                                let tx = connection.transaction().unwrap();

                                resolve_signature(&tx, block_id)
                            }).await.context("Joining blocking task").and_then(|res| res);

                            match signature {
                                Ok(signature) => {
                                    Ok(warp::reply::with_status(warp::reply::json(&signature), warp::http::StatusCode::OK))
                                },
                                Err(e) => {
                                    tracing::error!("Error fetching signature: {:?}", e);
                                    let error = serde_json::json!({"code": "StarknetErrorCode.BLOCK_NOT_FOUND", "message": "Block number not found"});
                                    Ok(warp::reply::with_status(warp::reply::json(&error), warp::http::StatusCode::BAD_REQUEST))
                                }
                            }
                        },
                        Err(_) => Err(warp::reject::reject()),
                    }
                }
            }
        });

    let get_state_update = warp::path("get_state_update")
        .and(warp::query::<BlockIdParam>())
        .and_then({
            let storage_rx = storage_rx.clone();
            let reorg_config = reorg_config.clone();
            let reorged = reorged.clone();

            move |block_id: BlockIdParam| {
                let storage_rx = storage_rx.clone();
                let reorg_config = reorg_config.clone();
                let reorged = reorged.clone();
                async move {
                    let include_block = block_id.include_block.unwrap_or(false);
                    let include_signature = block_id.include_signature.unwrap_or(false);

                    match block_id.try_into() {
                        Ok(block_id) => {
                            let Some(storage) = maybe_storage(storage_rx) else {
                                return Err(storage_unavailable_rejection());
                            };

                            let resolved = tokio::task::spawn_blocking(move || {
                                let mut connection = storage.connection().unwrap();
                                let tx = connection.transaction().unwrap();

                                let state_update = resolve_state_update(&tx, block_id, &reorg_config, reorged.clone())?;
                                let block = if include_block {
                                    Some(resolve_block(&tx, block_id, &reorg_config, reorged.clone())?)
                                } else {
                                    None
                                };
                                let signature = if include_signature {
                                    Some(resolve_signature(&tx, block_id)?)
                                } else {
                                    None
                                };
                                anyhow::Ok((state_update, block, signature))
                            }).await.context("Joining blocking task").and_then(|res| res);

                            match resolved {
                                Ok((state_update, block, signature)) => {
                                    #[derive(Serialize)]
                                    struct Reply {
                                        state_update: starknet_gateway_types::reply::StateUpdate,
                                        #[serde(skip_serializing_if = "Option::is_none")]
                                        block: Option<starknet_gateway_types::reply::Block>,
                                        #[serde(skip_serializing_if = "Option::is_none")]
                                        signature: Option<[BlockCommitmentSignatureElem; 2]>,
                                    }

                                    let reply = Reply {
                                        state_update,
                                        block,
                                        signature: signature.map(|s| s.signature),
                                    };

                                    Ok(warp::reply::with_status(warp::reply::json(&reply), warp::http::StatusCode::OK))
                                },
                                Err(_) => {
                                    let error = serde_json::json!({"code": "StarknetErrorCode.BLOCK_NOT_FOUND", "message": "Block number not found"});
                                    Ok(warp::reply::with_status(warp::reply::json(&error), warp::http::StatusCode::BAD_REQUEST))
                                }
                            }
                        },
                        Err(_) => Err(warp::reject::reject()),
                    }
                }
            }
        });

    let get_public_key = warp::path("get_public_key").then(|| async {
        warp::reply::json(&serde_json::json!(
            "0x1252b6bce1351844c677869c6327e80eae1535755b611c66b8f46e595b40eea"
        ))
    });

    #[derive(Debug, Deserialize)]
    struct ClassHashParam {
        #[serde(rename = "classHash")]
        class_hash: ClassHash,
    }

    let get_class_by_hash = warp::path("get_class_by_hash")
        .and(warp::query::<ClassHashParam>())
        .then({
            let storage_rx = storage_rx.clone();
            move |class_hash: ClassHashParam| {
                let storage_rx = storage_rx.clone();
                async move {
                    let Some(storage) = maybe_storage(storage_rx) else {
                        return Ok(storage_unavailable_response());
                    };

                    let class = tokio::task::spawn_blocking(move || {
                        let mut connection = storage.connection().unwrap();
                        let tx = connection.transaction().unwrap();

                        resolve_class(&tx, class_hash.class_hash)
                    }).await.context("Joining blocking task").and_then(|res| res);

                    match class {
                        Ok(class) => {
                            let response = warp::http::Response::builder().header("content-type", "application/json").body(class.into_vec()).unwrap();
                            Result::<_, Infallible>::Ok(response)
                        },
                        Err(_) => {
                            let error = r#"{"code": "StarknetErrorCode.UNDECLARED_CLASS", "message": "Class not found"}"#;
                            let response = warp::http::Response::builder().status(warp::http::StatusCode::BAD_REQUEST).body(error.as_bytes().to_owned()).unwrap();
                            Ok(response)
                        }
                    }
                }
            }
        });

    #[derive(Debug, Deserialize)]
    struct CompiledClassHashParam {
        #[serde(rename = "classHash")]
        class_hash: ClassHash,
    }

    let get_compiled_class_by_hash = warp::path("get_compiled_class_by_class_hash")
        .and(warp::query::<CompiledClassHashParam>())
        .then({
            let storage_rx = storage_rx.clone();

            move |compiled_class_hash: CompiledClassHashParam| {
                let storage_rx = storage_rx.clone();

                async move {
                    let Some(storage) = maybe_storage(storage_rx) else {
                        return Ok(storage_unavailable_response());
                    };

                    let compiled_class = tokio::task::spawn_blocking(move || {
                        let mut connection = storage.connection().unwrap();
                        let tx = connection.transaction().unwrap();

                        resolve_compiled_class(&tx, compiled_class_hash.class_hash)
                    }).await.context("Joining blocking task").and_then(|res| res);

                    match compiled_class {
                        Ok(compiled_class) => {
                            let response = warp::http::Response::builder().header("content-type", "application/json").body(compiled_class.into_vec()).unwrap();
                            Result::<_, Infallible>::Ok(response)
                        },
                        Err(_) => {
                            let error = r#"{"code": "StarknetErrorCode.UNDECLARED_CLASS", "message": "Class not found"}"#;
                            let response = warp::http::Response::builder().status(warp::http::StatusCode::BAD_REQUEST).body(error.as_bytes().to_owned()).unwrap();
                            Ok(response)
                        }
                    }
                }
            }
        });

    let handler = warp::get()
        .and(warp::path("feeder_gateway"))
        .and(
            get_block
                .or(get_state_update)
                .or(get_contract_addresses)
                .or(get_class_by_hash)
                .or(get_compiled_class_by_hash)
                .or(get_signature)
                .or(get_public_key),
        )
        .with(warp::filters::trace::request());

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], cli.port)))
        .await
        .context("Binding server")?;
    let socket_addr = listener.local_addr().context("Getting local address")?;
    let server = warp::serve(handler).incoming(listener);
    let span = tracing::info_span!("Server::run", ?socket_addr);
    tracing::info!(parent: &span, "listening on http://{}", socket_addr);

    let data_directory = cli
        .database_path
        .parent()
        .context("Getting database parent directory")?;

    debug_create_port_marker_file("feeder_gateway", socket_addr.port(), data_directory);

    server.run().instrument(span).await;

    Ok(())
}

fn get_chain(tx: &pathfinder_storage::Transaction<'_>) -> anyhow::Result<Chain> {
    use pathfinder_common::consts::{
        MAINNET_GENESIS_HASH,
        SEPOLIA_INTEGRATION_GENESIS_HASH,
        SEPOLIA_TESTNET_GENESIS_HASH,
    };

    let genesis_hash = tx
        .block_hash(BlockNumber::GENESIS.into())
        .unwrap()
        .context("Getting genesis hash")?;

    let chain = match genesis_hash {
        MAINNET_GENESIS_HASH => Chain::Mainnet,
        SEPOLIA_TESTNET_GENESIS_HASH => Chain::SepoliaTestnet,
        SEPOLIA_INTEGRATION_GENESIS_HASH => Chain::SepoliaIntegration,
        _other => Chain::Custom,
    };

    Ok(chain)
}

fn contract_addresses(chain: Chain) -> anyhow::Result<ContractAddresses> {
    fn parse(hex: &str) -> H160 {
        let slice: [u8; 20] = const_decoder::Decoder::Hex.decode(hex.as_bytes());
        H160::from(slice)
    }

    Ok(match chain {
        Chain::Mainnet => ContractAddresses {
            core: parse("c662c410C0ECf747543f5bA90660f6ABeBD9C8c4"),
            gps: parse("47312450B3Ac8b5b8e247a6bB6d523e7605bDb60"),
        },
        Chain::SepoliaTestnet | Chain::Custom => ContractAddresses {
            core: parse("E2Bb56ee936fd6433DC0F6e7e3b8365C906AA057"),
            gps: parse("07ec0D28e50322Eb0C159B9090ecF3aeA8346DFe"),
        },
        Chain::SepoliaIntegration => ContractAddresses {
            core: parse("4737c0c1B4D5b1A687B42610DdabEE781152359c"),
            gps: parse("07ec0D28e50322Eb0C159B9090ecF3aeA8346DFe"),
        },
    })
}

/// Groups the Starknet contract addresses for a specific chain.
///
/// Getting addresses: <SEQUENCER_URL>/feeder_gateway/get_contract_addresses
pub struct ContractAddresses {
    pub core: H160,
    pub gps: H160,
}

#[tracing::instrument(level = "trace", skip(tx))]
fn resolve_block(
    tx: &pathfinder_storage::Transaction<'_>,
    block_id: BlockId,
    reorg_config: &Option<ReorgConfig>,
    reorged: Arc<AtomicBool>,
) -> anyhow::Result<starknet_gateway_types::reply::Block> {
    let resolved_block_id =
        resolve_block_id(block_id, reorg_config, reorged).context("Resolving block id")?;

    let header = tx
        .block_header(resolved_block_id)
        .context("Fetching block header")?
        .context("Block header missing")?;

    let transactions_receipts = tx
        .transaction_data_for_block(header.number.into())
        .context("Reading transactions from database")?
        .context("Transaction data missing")?;

    let receipts = transactions_receipts
        .iter()
        .map(|(_, r, _)| r.clone())
        .collect::<Vec<_>>();

    let receipt_commitment = calculate_receipt_commitment(&receipts)?;

    let (transactions, transaction_receipts): (Vec<_>, Vec<_>) = transactions_receipts
        .into_iter()
        .map(|(tx, rx, ev)| (tx, (rx, ev)))
        .unzip();

    let (transaction_commitment, event_commitment) =
        if header.starknet_version < StarknetVersion::V_0_13_2 {
            // This needs to be re-calculated because we _always_ store 0.13.2 commitments
            // in the DB for P2P sync purposes
            let transaction_commitment =
                calculate_transaction_commitment(&transactions, header.starknet_version)?;
            let events: Vec<_> = transaction_receipts
                .iter()
                .map(|(receipt, events)| (receipt.transaction_hash, events.as_slice()))
                .collect();
            let event_commitment = calculate_event_commitment(&events, header.starknet_version)?;
            (transaction_commitment, event_commitment)
        } else {
            (header.transaction_commitment, header.event_commitment)
        };

    let block_status = tx
        .block_is_l1_accepted(header.number.into())
        .context("Querying block status")?;
    let block_status = if block_status {
        Status::AcceptedOnL1
    } else {
        Status::AcceptedOnL2
    };

    // If the block id was reorged, we should return a non-matching block hash
    // to trigger reorg detection in Pathfinder.
    let block_hash = if block_id != resolved_block_id {
        BlockHash::ZERO
    } else {
        header.hash
    };

    Ok(starknet_gateway_types::reply::Block {
        block_hash,
        block_number: header.number,
        l1_gas_price: GasPrices {
            price_in_wei: header.eth_l1_gas_price,
            price_in_fri: header.strk_l1_gas_price,
        },
        l1_data_gas_price: GasPrices {
            price_in_wei: header.eth_l1_data_gas_price,
            price_in_fri: header.strk_l1_data_gas_price,
        },
        l2_gas_price: Some(GasPrices {
            price_in_wei: header.eth_l2_gas_price,
            price_in_fri: header.strk_l2_gas_price,
        }),
        parent_block_hash: header.parent_hash,
        sequencer_address: Some(header.sequencer_address),
        state_commitment: header.state_commitment,
        status: block_status,
        timestamp: header.timestamp,
        transaction_receipts,
        transactions,
        starknet_version: header.starknet_version,
        l1_da_mode: header.l1_da_mode.into(),
        transaction_commitment,
        event_commitment,
        receipt_commitment: Some(receipt_commitment),
        state_diff_commitment: Some(header.state_diff_commitment),
        state_diff_length: Some(header.state_diff_length),
    })
}

#[tracing::instrument(level = "trace", skip(tx))]
fn resolve_signature(
    tx: &pathfinder_storage::Transaction<'_>,
    block_id: BlockId,
) -> anyhow::Result<starknet_gateway_types::reply::BlockSignature> {
    let header = tx
        .block_header(block_id)
        .context("Fetching block header")?
        .context("Block header missing")?;

    let signature = tx
        .signature(block_id)
        .context("Fetching signature")?
        // fall back to zero since we might have missing signatures in old DBs
        .unwrap_or(BlockCommitmentSignature {
            r: BlockCommitmentSignatureElem::ZERO,
            s: BlockCommitmentSignatureElem::ZERO,
        });

    Ok(starknet_gateway_types::reply::BlockSignature {
        block_hash: header.hash,
        signature: [signature.r, signature.s],
    })
}

/// Apply reorg transformation to block id.
fn resolve_block_id(
    block_id: BlockId,
    reorg_config: &Option<ReorgConfig>,
    reorged: Arc<AtomicBool>,
) -> anyhow::Result<BlockId> {
    let block_id = if let Some(reorg_config) = reorg_config {
        match block_id {
            BlockId::Number(block_number) => {
                if reorged.load(Ordering::Relaxed) {
                    // reorg is active
                    if block_number > reorg_config.reorg_to_block {
                        anyhow::bail!("Reorged block requested");
                    }
                    reorged.store(false, Ordering::Relaxed);
                } else {
                    // reorg should start at this block
                    if block_number > reorg_config.reorg_at_block {
                        tracing::warn!(%reorg_config.reorg_to_block, "Reorg");
                        reorged.store(true, Ordering::Relaxed);
                        anyhow::bail!("Reorg happened");
                    }
                }

                block_id
            }
            BlockId::Latest => {
                if reorged.load(Ordering::Relaxed) {
                    reorg_config.reorg_to_block.into()
                } else {
                    block_id
                }
            }
            _ => block_id,
        }
    } else {
        block_id
    };

    Ok(block_id)
}

#[tracing::instrument(level = "trace", skip(tx))]
fn resolve_state_update(
    tx: &pathfinder_storage::Transaction<'_>,
    block: BlockId,
    reorg_config: &Option<ReorgConfig>,
    reorged: Arc<AtomicBool>,
) -> anyhow::Result<starknet_gateway_types::reply::StateUpdate> {
    let block = resolve_block_id(block, reorg_config, reorged).context("Resolving block id")?;

    tx.state_update(block)
        .context("Fetching state update")?
        .context("State update missing")
        .map(storage_to_gateway)
}

#[tracing::instrument(level = "trace", skip(tx))]
fn resolve_class(
    tx: &pathfinder_storage::Transaction<'_>,
    class_hash: ClassHash,
) -> anyhow::Result<SerializedOpaqueClassDefinition> {
    let definition = tx
        .class_definition(class_hash)
        .context("Reading class definition from database")?
        .context("No such class found")?;

    Ok(definition)
}

#[tracing::instrument(level = "trace", skip(tx))]
fn resolve_compiled_class(
    tx: &pathfinder_storage::Transaction<'_>,
    compiled_class_hash: ClassHash,
) -> anyhow::Result<SerializedCasmDefinition> {
    let definition = tx
        .casm_definition(compiled_class_hash)
        .context("Reading compiled class definition from database")?
        .context("No such class found")?;

    Ok(definition)
}

fn storage_to_gateway(
    state_update: pathfinder_common::StateUpdate,
) -> starknet_gateway_types::reply::StateUpdate {
    let mut storage_diffs = HashMap::new();
    let mut deployed_contracts = Vec::new();
    let mut nonces = HashMap::new();
    let mut replaced_classes = Vec::new();

    for (address, update) in state_update.contract_updates {
        if let Some(nonce) = update.nonce {
            nonces.insert(address, nonce);
        }

        match update.class {
            Some(ContractClassUpdate::Deploy(class_hash)) => {
                deployed_contracts.push(DeployedContract {
                    address,
                    class_hash,
                })
            }
            Some(ContractClassUpdate::Replace(class_hash)) => {
                replaced_classes.push(ReplacedClass {
                    address,
                    class_hash,
                })
            }
            None => {}
        }

        let storage = update
            .storage
            .into_iter()
            .map(|(key, value)| StorageDiff { key, value })
            .collect();

        storage_diffs.insert(address, storage);
    }

    for (address, update) in state_update.system_contract_updates {
        let storage = update
            .storage
            .into_iter()
            .map(|(key, value)| StorageDiff { key, value })
            .collect();

        storage_diffs.insert(address, storage);
    }

    let declared_classes = state_update
        .declared_sierra_classes
        .into_iter()
        .map(|(class_hash, compiled_class_hash)| DeclaredSierraClass {
            class_hash,
            compiled_class_hash,
        })
        .collect();

    let migrated_compiled_classes = state_update
        .migrated_compiled_classes
        .into_iter()
        .map(|(sierra_hash, casm_hash)| MigratedCompiledClass {
            class_hash: sierra_hash,
            compiled_class_hash: casm_hash,
        })
        .collect();

    let state_diff = starknet_gateway_types::reply::state_update::StateDiff {
        storage_diffs,
        deployed_contracts,
        old_declared_contracts: state_update.declared_cairo_classes,
        declared_classes,
        nonces,
        replaced_classes,
        migrated_compiled_classes,
    };

    starknet_gateway_types::reply::StateUpdate {
        block_hash: state_update.block_hash,
        new_root: state_update.state_commitment,
        old_root: state_update.parent_state_commitment,
        state_diff,
    }
}
