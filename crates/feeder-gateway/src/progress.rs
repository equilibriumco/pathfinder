use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, SystemTime};

#[derive(Default)]
pub struct Progress {
    block_duration_sec: u16,
    latest_now: Option<u64>,
    // has to be per-session to send unchanged responses only to
    // clients who did previously receive a full response
    preconfirmed_now: HashMap<String, u64>,
    response_counter: u64,
    available_since: Option<SystemTime>,
}

impl Progress {
    pub fn new(latest: Option<u64>) -> Self {
        Self {
            block_duration_sec: 2,
            latest_now: latest,
            preconfirmed_now: HashMap::new(),
            response_counter: 0,
            available_since: None,
        }
    }

    pub fn timed_latest(&mut self) -> Option<u64> {
        if let Some(latest_now) = self.latest_now {
            if self.update_time() {
                self.latest_now = Some(latest_now + 1);
            }

            self.latest_now
        } else {
            None
        }
    }

    pub fn timed_preconfirmed(&mut self, identifier: &str) -> Option<(u64, String)> {
        if let Some(latest) = self.timed_latest() {
            let update = if let Some(last_preconfirmed) = self.read_preconfirmed(identifier) {
                last_preconfirmed != latest + 2
            } else {
                true
            };
            if update {
                let preconfirmed_now = latest + 2;
                self.response_counter += 1;
                let new_identifier = format!("0x{:x}", self.response_counter);
                self.preconfirmed_now.remove(identifier);
                self.preconfirmed_now
                    .insert(new_identifier.clone(), preconfirmed_now);
                Some((preconfirmed_now, new_identifier))
            } else {
                // previous returned value still valid
                None
            }
        } else {
            tracing::error!("Progress simulation not configured");
            None
        }
    }

    fn read_preconfirmed(&self, identifier: &str) -> Option<u64> {
        if identifier.is_empty() {
            None
        } else {
            self.preconfirmed_now.get(identifier).copied()
        }
    }

    fn update_time(&mut self) -> bool {
        let now = SystemTime::now();
        if let Some(old_since) = self.available_since {
            let rel_secs = now.duration_since(old_since).unwrap().as_secs_f64();
            if rel_secs >= self.block_duration_sec.into() {
                self.available_since =
                    Some(old_since.add(Duration::from_secs(self.block_duration_sec.into())));
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
}
