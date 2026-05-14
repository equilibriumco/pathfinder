//! Utilities for spawning and managing Pathfinder instances.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use futures::future::Either;
use http::StatusCode;
use pathfinder_common::integration_testing::debug_create_marker_file;
use pathfinder_lib::config::integration_testing::InjectFailureConfig;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::common::utils;

/// Represents a running Pathfinder instance.
pub struct PathfinderInstance {
    process: Child,
    name: &'static str,
    monitor_port: AtomicU16,
    consensus_p2p_port: AtomicU16,
    rpc_port_watch_tx: watch::Sender<(u32, u16)>,
    rpc_port_watch_rx: watch::Receiver<(u32, u16)>,
    db_dir: PathBuf,
    /// `true` if [`PathfinderInstance::exited_wit_error`] returned
    /// `Ok(_)`.
    is_terminated: bool,
}

/// Configuration for a Pathfinder instance.
#[derive(Clone)]
pub struct Config {
    pub name: &'static str,
    pub boot_peers: Vec<BootstrapPeerConfig>,
    pub sync_enabled: bool,
    pub my_validator_address: u8,
    pub validator_addresses: Vec<u8>,
    pub pathfinder_bin: PathBuf,
    pub fixture_dir: PathBuf,
    pub test_dir: PathBuf,
    pub inject_failure: Option<InjectFailureConfig>,
    pub local_feeder_gateway_port: Option<u16>,
    pub boot_db: Option<PathBuf>,
}

#[derive(Clone)]
pub struct BootstrapPeerConfig {
    pub port: u16,
    pub id: p2p::libp2p::PeerId,
}

pub type RpcPortWatch = (watch::Sender<(u32, u16)>, watch::Receiver<(u32, u16)>);

impl PathfinderInstance {
    /// Spawns a new Pathfinder instance with the given configuration. The
    /// instance is not guaranteed to be ready after this function returns.
    /// Use [`wait_for_ready`](PathfinderInstance::wait_for_ready) to wait until
    /// the instance is ready to accept requests.
    ///
    /// # Important
    ///
    /// The spawned instance will be terminated when the returned
    /// [`PathfinderInstance`] is dropped, unless
    /// [`exited_with_error`](PathfinderInstance::exited_with_error) already
    /// returned `Ok(_)`, which means that the instance has already exited.
    pub fn spawn(config: Config) -> anyhow::Result<Self> {
        let id_file = config.fixture_dir.join(format!("id_{}.json", config.name));
        let db_dir = config.db_dir();

        if let Some(source) = &config.boot_db {
            let destination = db_dir.join("custom.sqlite");
            std::fs::create_dir_all(&db_dir).context("Creating db directory")?;
            std::fs::copy(source, &destination).context(format!(
                "Copying bootstrap DB from {} to {}",
                source.display(),
                destination.display(),
            ))?;
        }

        let stdout_path = config.test_dir.join(format!("{}_stdout.log", config.name));
        let stdout_file =
            utils::create_log_file(format!("Pathfinder instance {}", config.name), &stdout_path)?;
        let stderr_path = config.test_dir.join(format!("{}_stderr.log", config.name));
        let stderr_file =
            utils::create_log_file(format!("Pathfinder instance {}", config.name), &stderr_path)?;

        let mut command = Command::new(config.pathfinder_bin);
        let command = command
            .stdout(stdout_file)
            .stderr(stderr_file)
            .env(
                "RUST_LOG",
                "pathfinder_lib=trace,pathfinder=trace,pathfinder_consensus=trace,p2p=off,\
                 informalsystems_malachitebft_core_consensus=trace,starknet_gateway_client=trace,\
                 starknet_gateway_types=trace,pathfinder_storage=trace",
            )
            .arg("--ethereum.url=wss://ethereum-sepolia-rpc.publicnode.com");

        if let Some(port) = config.local_feeder_gateway_port {
            command.args([
                "--network=custom",
                "--chain-id=SN_SEPOLIA",
                &format!("--feeder-gateway-url=http://127.0.0.1:{port}/feeder_gateway"),
                &format!("--gateway-url=http://127.0.0.1:{port}/gateway"),
            ]);
        } else {
            command.arg("--network=sepolia-testnet");
        }

        let command = command.args([
            format!("--data-directory={}", db_dir.display()).as_str(),
            "--debug.pretty-log=true",
            "--color=never",
            "--monitor-address=127.0.0.1:0",
            "--rpc.enable=true",
            "--http-rpc=127.0.0.1:0",
            "--consensus.enable=true",
            // Currently the proposer address always points to Alice (0x1).
            "--consensus.proposer-addresses=0x1",
            format!(
                "--consensus.my-validator-address={:#x}",
                config.my_validator_address
            )
            .as_str(),
            format!(
                "--consensus.validator-addresses={}",
                config
                    .validator_addresses
                    .iter()
                    .map(|a| format!("{a:#x}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .as_str(),
            "--consensus.history-depth=2",
            format!("--p2p.consensus.identity-config-file={}", id_file.display()).as_str(),
            "--p2p.consensus.listen-on=/ip4/127.0.0.1/tcp/0",
            "--p2p.consensus.experimental.direct-connection-timeout=1",
            "--p2p.consensus.experimental.eviction-timeout=1",
        ]);
        let boot_peer_addrs: Vec<_> = config
            .boot_peers
            .iter()
            .map(|boot_peer| {
                format!(
                    "/ip4/127.0.0.1/tcp/{}/p2p/{}",
                    boot_peer.port,
                    boot_peer.id.to_base58()
                )
            })
            .collect();
        if !boot_peer_addrs.is_empty() {
            command.arg(format!(
                "--p2p.consensus.bootstrap-addresses={}",
                boot_peer_addrs.join(",")
            ));
        }
        command.arg(format!("--sync.enable={}", config.sync_enabled));
        command.args([
            "--integration-tests.disable-db-verification=true",
            "--integration-tests.disable-gas-price-validation=true",
        ]);

        config.inject_failure.map(|i| {
            command.arg(format!(
                "--integration-tests.inject-failure={},{}",
                i.height,
                i.trigger.as_str()
            ))
        });

        let process = command
            .spawn()
            .context(format!("Spawning pathfinder process for {}", config.name))?;

        println!(
            "Pathfinder instance {:<7} (pid: {}) has been spawned",
            config.name,
            process.id()
        );

        let (rpc_port_watch_tx, rpc_port_watch_rx) = watch::channel((0u32, 0u16));
        // Keep the refcount at at least 1 to achieve 'static lifetime, especially when
        // an instance is terminated and then respawned.
        let rpc_port_watch_tx2 = rpc_port_watch_tx.clone();
        _ = Box::leak(Box::new(rpc_port_watch_tx2));

        Ok(Self {
            process,
            name: config.name,
            monitor_port: AtomicU16::new(0),
            consensus_p2p_port: AtomicU16::new(0),
            rpc_port_watch_tx,
            rpc_port_watch_rx,
            db_dir,
            is_terminated: false,
        })
    }

    pub fn with_rpc_port_watch(mut self, (tx, rx): RpcPortWatch) -> Self {
        self.rpc_port_watch_tx = tx;
        self.rpc_port_watch_rx = rx;
        self
    }

    /// Checks if the instance has exited with a non-zero exit code.
    pub fn exited_with_error(&mut self) -> anyhow::Result<bool> {
        match self.process.try_wait() {
            // The process has exited with some error exit code.
            Ok(Some(status)) if !status.success() => {
                self.is_terminated = true;
                // The previous port value is invalid, and the new Pathfinder instance hasn't
                // been spawned yet.
                self.rpc_port_watch_tx
                    .send((0, 0))
                    .context("Clearing rpc port")?;
                Ok(true)
            }
            // The process has exited with 0 code.
            Ok(Some(_)) => {
                self.is_terminated = true;
                // The previous port value is invalid, and the new Pathfinder instance hasn't
                // been spawned yet.
                self.rpc_port_watch_tx
                    .send((0, 0))
                    .context("Clearing rpc port")?;
                Ok(false)
            }
            // The process is still running. The previous port value is still valid.
            Ok(None) => Ok(false),
            // An error occurred while checking the process status.
            Err(e) => Err(anyhow::anyhow!(
                "Error checking if Pathfinder instance {} (pid: {}) has exited: {e}",
                self.name,
                self.process.id()
            )),
        }
    }

    /// Waits until the instance is ready to accept requests on the monitor,
    /// rpc, and consensus p2p ports, or until `timeout` is reached. Polls
    /// every `poll_interval`. If the timeout is reached, an error is
    /// returned.
    pub async fn wait_for_ready(
        &self,
        poll_interval: Duration,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let pid = self.process.id();
        let fut = async move {
            let stopwatch = Instant::now();
            let (monitor_port, rpc_port, p2p_port) = tokio::join!(
                utils::wait_for_port(pid, "monitor", &self.db_dir, poll_interval),
                utils::wait_for_port(pid, "rpc", &self.db_dir, poll_interval),
                utils::wait_for_port(pid, "p2p_consensus", &self.db_dir, poll_interval),
            );
            let monitor_port = monitor_port?;
            self.monitor_port.store(monitor_port, Ordering::Relaxed);
            self.rpc_port_watch_tx
                .send((pid, rpc_port?))
                .context("Updating rpc port")?;
            self.consensus_p2p_port.store(p2p_port?, Ordering::Relaxed);

            loop {
                match reqwest::get(format!("http://127.0.0.1:{monitor_port}/ready")).await {
                    Ok(response) if response.status() == StatusCode::OK => {
                        println!(
                            "Pathfinder instance {:<7} (pid: {pid}) ready after {} s",
                            self.name,
                            stopwatch.elapsed().as_secs()
                        );
                        return anyhow::Ok(());
                    }
                    _ => println!(
                        "Pathfinder instance {:<7} (pid: {pid}) not ready yet",
                        self.name,
                    ),
                }

                sleep(poll_interval).await;
            }
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(_)) => {
                // This is to let other processes know
                debug_create_marker_file(&format!("{}_ready", self.name), &self.db_dir);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                anyhow::bail!(
                    "Timeout waiting for Pathfinder instance {} (pid: {pid}) to be ready",
                    self.name
                )
            }
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn rpc_port_watch_rx(&self) -> watch::Receiver<(u32, u16)> {
        self.rpc_port_watch_rx.clone()
    }

    pub fn rpc_port_watch(&self) -> RpcPortWatch {
        (
            self.rpc_port_watch_tx.clone(),
            self.rpc_port_watch_rx.clone(),
        )
    }

    pub fn consensus_p2p_port(&self) -> u16 {
        self.consensus_p2p_port.load(Ordering::Relaxed)
    }

    fn terminate(&mut self) {
        if self.is_terminated {
            println!(
                "Pathfinder instance {:<7} (pid: {}) has already exited",
                self.name,
                self.process.id()
            );
            return;
        }

        utils::terminate(
            &mut self.process,
            format!("Pathfinder instance {:<7}", self.name),
        );
    }
}

impl Drop for PathfinderInstance {
    fn drop(&mut self) {
        self.terminate();
    }
}

impl Config {
    const NAMES: &'static [&'static str] = &[
        "Alice", "Bob", "Charlie", "Dan", "Eve", "Frank", "Grace", "Heidi",
    ];

    /// The first node will always be the boot node.
    pub fn for_set(
        set_size: usize,
        pathfinder_bin: &Path,
        fixture_dir: &Path,
        test_dir: PathBuf,
        boot_db: Option<PathBuf>,
    ) -> Vec<Self> {
        assert!(
            set_size <= Self::NAMES.len(),
            "Max {} instances supported",
            Self::NAMES.len()
        );
        (0..set_size)
            .map(|i| Self {
                name: Self::NAMES[i],
                boot_peers: Vec::new(),
                sync_enabled: false,
                my_validator_address: (i + 1) as u8,
                // The set is deduplicated when consensus task is started, so including the own
                // validator address is fine.
                validator_addresses: (1..=set_size as u8).collect::<Vec<_>>(),
                test_dir: test_dir.clone(),
                pathfinder_bin: pathfinder_bin.to_path_buf(),
                fixture_dir: fixture_dir.to_path_buf(),
                inject_failure: None,
                local_feeder_gateway_port: None,
                boot_db: boot_db.clone(),
            })
            .collect()
    }

    pub fn with_inject_failure(mut self, inject_failure: Option<InjectFailureConfig>) -> Self {
        self.inject_failure = inject_failure;
        self
    }

    pub fn with_boot_peer(mut self, port: u16, peer_id: p2p::libp2p::PeerId) -> Self {
        self.boot_peers
            .push(BootstrapPeerConfig { port, id: peer_id });
        self
    }

    pub fn with_sync_enabled(mut self) -> Self {
        self.sync_enabled = true;
        self
    }

    pub fn with_local_feeder_gateway(mut self, port: u16) -> Self {
        self.local_feeder_gateway_port = Some(port);
        self
    }

    pub fn db_dir(&self) -> PathBuf {
        self.test_dir.join(format!("db-{}", self.name))
    }
}

/// A guard that aborts the task when dropped.
pub struct AbortGuard {
    jh: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

impl From<JoinHandle<anyhow::Result<()>>> for AbortGuard {
    fn from(jh: JoinHandle<anyhow::Result<()>>) -> Self {
        Self { jh }
    }
}

pub struct MaybeRespawned(
    Either<(AbortGuard, mpsc::Receiver<PathfinderInstance>), PathfinderInstance>,
);

impl MaybeRespawned {
    pub fn instance(self) -> Option<PathfinderInstance> {
        match self.0 {
            Either::Left((_, mut rx)) => rx.try_recv().ok(),
            Either::Right(instance) => Some(instance),
        }
    }
}

/// Monitors `instance` for exit with non-zero exit code. If that happens,
/// respawns the instance with `config` and waits for it to be ready.
pub fn respawn_on_fail(
    inject_failure: bool,
    mut instance: PathfinderInstance,
    config: Config,
    ready_poll_interval: Duration,
    ready_timeout: Duration,
) -> MaybeRespawned {
    if !inject_failure {
        return MaybeRespawned(Either::Right(instance));
    }

    let mut child_signal = signal(SignalKind::child()).unwrap();
    let (tx, rx) = mpsc::channel(1);
    let abort_guard = tokio::spawn(async move {
        if child_signal.recv().await.is_some() {
            println!("Got SIGCHLD!");
            match instance.exited_with_error() {
                Ok(true) => {
                    println!("Respawning {}...", instance.name());
                    let watch = instance.rpc_port_watch();
                    drop(instance);
                    let instance = PathfinderInstance::spawn(config)?.with_rpc_port_watch(watch);
                    instance
                        .wait_for_ready(ready_poll_interval, ready_timeout)
                        .await?;
                    println!("{} is ready again", instance.name());
                    let _ = tx.send(instance).await;
                }
                Ok(false) => {
                    println!("{} exited cleanly, not respawning", instance.name());
                    drop(instance);
                }
                Err(e) => {
                    eprintln!("Error checking if {} exited cleanly: {e}", instance.name());
                    // Assume that the process did not exit, the worst that can
                    // happen is that the kill on drop will fail.
                }
            }
        }

        Ok(())
    })
    .into();

    MaybeRespawned(Either::Left((abort_guard, rx)))
}
