use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use serde::Deserialize;

#[derive(clap::ValueEnum, Clone, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Network {
    Mainnet,
    SepoliaTestnet,
    SepoliaIntegration,
}

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[arg(
        long = "network",
        long_help = "Starknet network of the gateway to record.",
        value_enum,
        default_value = "sepolia-testnet"
    )]
    network: Network,

    #[arg(
        long,
        long_help = "Number of requests to the real gateway.",
        default_value = "100"
    )]
    total_requests: usize,

    #[arg(long, long_help = "Pause between requests.", default_value = "100")]
    sleep_ms: u64,

    #[arg(
        long,
        long_help = "Output directory for the recorded responses.",
        default_value = "record"
    )]
    output_dir: PathBuf,
}

impl Network {
    fn feeder_gateway(&self) -> &'static str {
        match self {
            Self::Mainnet => "https://feeder.alpha-mainnet.starknet.io/feeder_gateway",
            Self::SepoliaTestnet => "https://feeder.alpha-sepolia.starknet.io/feeder_gateway",
            Self::SepoliaIntegration => {
                "https://feeder.integration-sepolia.starknet.io/feeder_gateway"
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PreConfirmedPollResponseSubset {
    pub changed: bool,

    #[serde(default)]
    pub block_identifier: Option<String>,

    #[serde(default)]
    pub block_number: Option<u64>,
}

pub struct PollState {
    pub block_identifier: String,
    pub block_number: u64,
    pub body: String,
}

impl PollState {
    pub fn save(&self, output_dir: &Path) -> anyhow::Result<()> {
        let name = format!("{}-{}.json", self.block_number, self.block_identifier);
        let path = output_dir.join(name);
        let mut file = fs::File::create(path)?;
        file.write_all(self.body.as_bytes())?;
        Ok(())
    }
}

/// Records pre-confirmed blocks from a live feeder gateway
///
/// to be replayed by the simulated one.
///
/// Usage:
/// `cargo run --release -p pathfinder --example feeder_gateway_record`
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let sleep_time = Duration::from_millis(cli.sleep_ms);
    fs::create_dir_all(&cli.output_dir)?;
    let client = reqwest::Client::new();
    let url = cli.network.feeder_gateway().to_owned()
        + "/get_preconfirmed_block?blockNumber=latest&blockIdentifier=&knownTransactionCount=0";
    let mut poll_state: Option<PollState> = None;
    for i in 0..cli.total_requests {
        if i > 0 {
            tokio::time::sleep(sleep_time).await;
        }

        let result = client.get(&url).send().await?;
        if result.status() != reqwest::StatusCode::OK {
            continue;
        }

        let body = result.text().await?;
        let response: PreConfirmedPollResponseSubset = serde_json::from_str(&body)?;
        if !response.changed {
            continue;
        }

        let Some(block_identifier) = response.block_identifier else {
            continue;
        };

        let Some(block_number) = response.block_number else {
            continue;
        };

        if let Some(ref cur_poll_state) = poll_state {
            if (cur_poll_state.block_identifier != block_identifier)
                || (cur_poll_state.block_number != block_number)
            {
                cur_poll_state.save(&cli.output_dir)?;
            }
        }

        poll_state = Some(PollState {
            block_identifier,
            block_number,
            body,
        });
    }

    Ok(())
}
