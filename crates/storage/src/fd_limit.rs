//! Process-wide `RLIMIT_NOFILE` soft-limit raise, observed once per
//! process lifetime. Lives next to the RocksDB open that wants the
//! budget so the dependency is not surprising.
//!
//! RocksDB's `max_open_files = -1` is the documented best-performance
//! setting (no eviction) — see the tuning guide, "General Options":
//! https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-Options#general-options
//! That setting is only safe if the process FD budget is generous,
//! which on Linux and macOS dev machines is usually not the default.

use std::sync::OnceLock;

static EFFECTIVE_SOFT: OnceLock<Option<u64>> = OnceLock::new();

/// Raise the process `RLIMIT_NOFILE` soft limit to the hard limit on
/// Unix and return the effective post-raise soft. Subsequent calls
/// return the cached value without re-running syscalls.
///
/// Returns `None` to signal "no information available".
pub(crate) fn raise_to_hard() -> Option<u64> {
    *EFFECTIVE_SOFT.get_or_init(raise_to_hard_impl)
}

#[cfg(unix)]
fn raise_to_hard_impl() -> Option<u64> {
    // SAFETY: regular FFI call.
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return None;
        }
        let pre_raise_soft = limit.rlim_cur;

        let raised = libc::rlimit {
            rlim_cur: limit.rlim_max,
            rlim_max: limit.rlim_max,
        };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &raised) != 0 {
            return Some(pre_raise_soft);
        }

        // macOS silently clamps the new soft at `kern.maxfilesperproc`,
        // so observe the effective value via a second `getrlimit`
        // rather than trusting the requested hard.
        let mut effective = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut effective) != 0 {
            return Some(pre_raise_soft);
        }
        Some(effective.rlim_cur)
    }
}

#[cfg(not(unix))]
fn raise_to_hard_impl() -> Option<u64> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn current_soft() -> u64 {
        // SAFETY: same shape as the production `getrlimit` call.
        unsafe {
            let mut l = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut l), 0);
            l.rlim_cur
        }
    }

    #[test]
    fn raise_is_idempotent_and_does_not_regress() {
        let before = current_soft();
        let first = raise_to_hard().expect("raise should succeed on Unix");
        let second = raise_to_hard().expect("raise should be cached on Unix");

        assert!(
            first >= before,
            "raise must not regress soft limit ({first} < {before})"
        );
        assert_eq!(first, second, "raise must be idempotent");
    }
}
