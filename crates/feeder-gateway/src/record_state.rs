use std::fs;
use std::ops::Add;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use pathfinder_common::BlockId;

#[derive(Default)]
pub struct RecordState {
    responses: Vec<serde_json::Value>,
    base_block_number: u64,
    available_since: Option<SystemTime>,
    next_response: usize,
}

impl RecordState {
    pub fn load(&mut self, record_dir: &PathBuf) -> anyhow::Result<()> {
        let mask_path = record_dir.join("*.json");
        let mask_path_str = mask_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid record dir: {:?}", record_dir))?;
        let mut base_block_number: Option<u64> = None;
        let mut running_block_number = 0;
        for entry in glob::glob(mask_path_str)? {
            let sample = entry?;
            let text = fs::read_to_string(sample)?;
            let json: serde_json::Value = serde_json::from_str(&text)?;

            let block_number = json
                .get("block_number")
                .and_then(serde_json::Value::as_u64)
                .context("missing block number")?;
            if base_block_number.is_none() {
                base_block_number = Some(block_number);
            } else {
                anyhow::ensure!(running_block_number + 1 == block_number);
            }
            running_block_number = block_number;

            self.responses.push(json);
        }

        if let Some(block_number) = base_block_number {
            self.base_block_number = block_number;
            Ok(())
        } else {
            Err(anyhow::anyhow!("no records"))
        }
    }

    pub fn get_next(&mut self, block_id: BlockId) -> anyhow::Result<serde_json::Value> {
        match block_id {
            BlockId::Latest => {
                if self.next_response < self.responses.len() {
                    let json = if self.update_time() {
                        self.set_timestamp();
                        let i = self.next_response;
                        self.next_response += 1;
                        self.responses[i].clone()
                    } else {
                        serde_json::json!({"changed": false})
                    };

                    Ok(json)
                } else {
                    Err(anyhow::anyhow!("no more records"))
                }
            }
            BlockId::Number(bn) => {
                let n = bn.to_i64() as u64;
                if n >= self.base_block_number {
                    let idx = n - self.base_block_number;
                    if idx < self.responses.len() as u64 {
                        return Ok(self.responses[idx as usize].clone());
                    }
                }

                Err(anyhow::anyhow!("block number out of range"))
            }
            _ => Err(anyhow::anyhow!(
                "pre-confirmed responses not implemented for specific blocks"
            )),
        }
    }

    fn update_time(&mut self) -> bool {
        let now = SystemTime::now();
        if let Some(old_since) = self.available_since {
            let rel_secs = now.duration_since(old_since).unwrap().as_secs_f64();
            if rel_secs >= 2.0 {
                self.available_since = Some(old_since.add(Duration::from_secs(2)));
                true
            } else {
                false
            }
        } else {
            let abs_secs = now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
                .trunc();
            self.available_since =
                Some(SystemTime::UNIX_EPOCH.add(Duration::from_secs_f64(abs_secs)));
            true
        }
    }

    fn set_timestamp(&mut self) {
        let since = self
            .available_since
            .expect("update_time to have been called");
        let ts = since
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.responses[self.next_response]["timestamp"] = ts.into();
    }
}
