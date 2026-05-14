//! Test utilities for Pathfinder integration tests.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use p2p_proto::common::Address;
use pathfinder_crypto::Felt;
use pathfinder_lib::devnet::{init_db, BootDb};
use serde::Deserialize;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;

use crate::common::pathfinder_instance::Config;

/// This function does a few things at the beginning of an integration test:
/// - creates temporary directory for test artifacts,
/// - verifies that the Pathfinder binary and fixtures directory exist,
/// - starts an [`std::time::Instant`] to measure test setup duration,
/// - returns configuration for the number of nodes specified and the instant.
pub fn setup(
    num_instances: usize,
    init_devnet_db: bool,
) -> anyhow::Result<(Vec<Config>, u64, Instant)> {
    let stopwatch = Instant::now();

    let pathfinder_bin = pathfinder_bin();
    anyhow::ensure!(pathfinder_bin.exists(), "Pathfinder binary not found");
    let fixture_dir = fixture_dir();
    anyhow::ensure!(fixture_dir.exists(), "Fixture directory not found");

    // CI uses `APP_TEMP_DIR`
    let tmp_path = std::env::var("APP_TEMP_DIR")
        .map(PathBuf::from)
        .unwrap_or(std::env::temp_dir());
    let parent_dir = tmp_path.join("consensus-integration-tests");
    std::fs::create_dir_all(&parent_dir)
        .context("Creating parent directory for all integration tests")?;

    let test_dir = TempDir::new_in(&parent_dir)
        .context("Creating temporary directory for test artifacts")?
        .keep();
    println!("Test artifacts will be stored in {}", test_dir.display());

    let (boot_db, num_boot_blocks) = if init_devnet_db {
        let BootDb {
            db_file_path,
            num_boot_blocks,
        } = init_db(&test_dir, Address(Felt::ONE /* Alice */))?;
        (Some(db_file_path), num_boot_blocks)
    } else {
        (None, 0)
    };

    Ok((
        Config::for_set(
            num_instances,
            &pathfinder_bin,
            &fixture_dir,
            test_dir,
            boot_db,
        ),
        num_boot_blocks,
        stopwatch,
    ))
}

/// Logs how many seconds have elapsed since `stopwatch` was created.
pub fn log_elapsed(stopwatch: Instant) {
    println!(
        "Test setup completed after {} s",
        stopwatch.elapsed().as_secs()
    );
}

/// Waits for either all RPC client tasks to complete, the timeout to elapse,
/// for the user to interrupt with Ctrl-C, or the rpc client to encounter an
/// error.
pub async fn join_all(
    rpc_client_handles: Vec<JoinHandle<()>>,
    test_timeout: Duration,
    mut err_rx: mpsc::Receiver<anyhow::Error>,
) -> anyhow::Result<()> {
    tokio::select! {
        _ = sleep(test_timeout) => {
            eprintln!("Test timed out after {test_timeout:?}");
            Err(anyhow::anyhow!("Test timed out after {test_timeout:?}"))
        }

        join_result = futures::future::join_all(rpc_client_handles) => {
            join_result.into_iter().collect::<Result<Vec<()>, JoinError>>().context("Joining all RPC client tasks")?;
            Ok(())
        }

        _ = tokio::signal::ctrl_c() => {
            eprintln!("Received Ctrl-C, terminating test early");
            Err(anyhow::anyhow!("Test interrupted by user"))
        }

        err = err_rx.recv() => {
            let err = err.expect("Error channel should not be closed");
            eprintln!("RPC client encountered an error: {err:#?}");
            Err(anyhow::anyhow!("RPC client encountered an error: {err:#?}"))
        }
    }
}

fn pathfinder_bin() -> PathBuf {
    let mut path = manifest_dir_path();
    assert!(path.pop());
    assert!(path.pop());
    path.push("target");
    path.push("debug");
    path.push("pathfinder");
    path
}

pub fn feeder_gateway_bin() -> PathBuf {
    let mut path = manifest_dir_path();
    assert!(path.pop());
    assert!(path.pop());
    path.push("target");
    path.push("debug");
    path.push("feeder-gateway");
    path
}

#[derive(Deserialize)]
pub struct IdFixture {
    #[allow(dead_code)]
    pub private_key: String,
    pub peer_id: p2p::libp2p::PeerId,
}

pub fn read_id_fixture(name: &str) -> anyhow::Result<IdFixture> {
    let path = fixture_dir().join(format!("id_{name}.json"));
    let mut buf = Vec::new();
    let mut file =
        File::open(&path).with_context(|| format!("Opening ID fixture {}", path.display()))?;
    file.read_to_end(&mut buf)
        .with_context(|| format!("Reading ID fixture {}", path.display()))?;
    serde_json::from_slice(&buf)
        .with_context(|| format!("Deserializing ID fixture {}", path.display()))
}

fn fixture_dir() -> PathBuf {
    let mut path = manifest_dir_path();
    path.push("tests");
    path.push("fixtures");
    path
}

fn manifest_dir_path() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
}

pub fn create_log_file(
    process_name: impl AsRef<str>,
    stdout_path: &Path,
) -> Result<File, anyhow::Error> {
    let stdout_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(stdout_path)
        .context(format!(
            "Creating log file {} for {}",
            stdout_path.display(),
            process_name.as_ref()
        ))?;
    Ok(stdout_file)
}

/// Terminates a child process gracefully using SIGTERM, and forcefully kills it
/// if necessary.
pub fn terminate(process: &mut Child, name: impl AsRef<str>) {
    let name = name.as_ref();
    println!("{name} (pid: {}) terminating...", process.id());

    _ = Command::new("kill")
        // It's supposed to be the default signal in `kill`, but let's be explicit.
        .arg("-TERM")
        .arg(process.id().to_string())
        .status();

    // See if SIGTERM worked.
    match process.try_wait() {
        Ok(Some(status)) => {
            println!(
                "{name} (pid: {}) terminated with status: {status}",
                process.id()
            );
        }
        Ok(None) => match process.wait() {
            Ok(status) => {
                println!(
                    "{name} (pid: {}) terminated with status: {status}",
                    process.id()
                );
            }
            Err(e) => {
                eprintln!(
                    "Error waiting for {name} (pid: {}) to terminate: {e}",
                    process.id()
                );
                if let Err(error) = process.kill() {
                    eprintln!("Error killing {name} (pid: {}): {error}", process.id(),);
                }
            }
        },
        Err(e) => {
            eprintln!("Error terminating {name} (pid: {}): {e}", process.id());
            if let Err(error) = process.kill() {
                eprintln!("Error killing {name} (pid: {}): {error}", process.id(),);
            }
        }
    }
}

/// Waits for the port marker file to appear and reads the port from it.
/// Polls every `poll_interval`.
pub async fn wait_for_port(
    pid: u32,
    port_name: &str,
    db_dir: &Path,
    poll_interval: Duration,
) -> anyhow::Result<u16> {
    let port_file = db_dir.join(format!("pid_{pid}_{port_name}_port"));

    loop {
        match tokio::fs::read_to_string(&port_file).await {
            Ok(port_str) => {
                let port = port_str
                    .trim()
                    .parse::<u16>()
                    .context(format!("Parsing port value in {}", port_file.display()))?;
                return Ok(port);
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // File not found yet, continue polling.
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error reading port file {}: {e}",
                    port_file.display()
                ));
            }
        }

        sleep(poll_interval).await;
    }
}
