use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use pathfinder_common::ContractAddress;
use pathfinder_consensus::ValidatorSet;

/// Height-keyed cache for consensus validator sets which are repeatedly fetched
/// while a height is active.
#[derive(Clone, Default)]
pub(crate) struct ValidatorCache {
    entries: Arc<RwLock<BTreeMap<u64, Arc<ValidatorSet<ContractAddress>>>>>,
}

impl ValidatorCache {
    /// Returns the cached value for `height`, or fetches and stores it on a
    /// cache miss.
    pub(crate) fn get_or_insert_with(
        &self,
        height: u64,
        max_cached_heights: usize,
        fetch: impl FnOnce() -> anyhow::Result<ValidatorSet<ContractAddress>>,
    ) -> anyhow::Result<Arc<ValidatorSet<ContractAddress>>> {
        if let Some(value) = self.entries.read().unwrap().get(&height).cloned() {
            return Ok(value);
        }

        let fetched = Arc::new(fetch()?);

        let mut entries = self.entries.write().unwrap();
        // Another thread may have inserted between our read and write locks.
        if let Some(existing) = entries.get(&height).cloned() {
            return Ok(existing);
        }
        entries.insert(height, fetched.clone());

        // Consensus advances monotonically, so when the cache is full we evict
        // the smallest key, which approximates LRU for the expected workload.
        while entries.len() > max_cached_heights {
            entries.pop_first();
        }

        Ok(fetched)
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_consensus::{PublicKey, Validator};

    use super::*;

    fn validator_set(address: ContractAddress) -> ValidatorSet<ContractAddress> {
        ValidatorSet::new([Validator {
            address,
            public_key: PublicKey::from_bytes([0; 32]),
            voting_power: 1,
        }])
    }

    fn cached_validator_set(
        cache: &ValidatorCache,
        height: u64,
    ) -> Arc<ValidatorSet<ContractAddress>> {
        cache
            .get_or_insert_with(height, 10, || Ok(validator_set(contract_address!("0x1"))))
            .unwrap()
    }

    #[test]
    fn repeat_lookup_at_same_height_hits_cache() {
        let cache = ValidatorCache::default();
        let first = cached_validator_set(&cache, 42);
        let second = cached_validator_set(&cache, 42);

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn distinct_heights_get_distinct_entries() {
        let cache = ValidatorCache::default();
        let a = cached_validator_set(&cache, 1);
        let b = cached_validator_set(&cache, 2);

        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn evicts_oldest_height_when_over_capacity() {
        let cache = ValidatorCache::default();
        let zero_first = cached_validator_set(&cache, 0);
        let max_cached_heights = 10;

        // Insert max_cached_heights more entries: at the last insertion the
        // map size hits max_cached_heights + 1 and the smallest key (0) is
        // evicted.
        for h in 1..=max_cached_heights as u64 {
            cached_validator_set(&cache, h);
        }
        let zero_again = cached_validator_set(&cache, 0);

        assert!(!Arc::ptr_eq(&zero_first, &zero_again));
    }
}
