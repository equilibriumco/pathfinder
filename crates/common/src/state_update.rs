use std::collections::{hash_map, HashMap, HashSet};
use std::slice;

use fake::Dummy;

use crate::{
    BlockHash,
    CasmHash,
    ClassHash,
    ContractAddress,
    ContractNonce,
    SierraHash,
    StateCommitment,
    StateDiffCommitment,
    StorageAddress,
    StorageValue,
};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct StateUpdate {
    pub block_hash: BlockHash,
    pub parent_state_commitment: StateCommitment,
    pub state_commitment: StateCommitment,
    pub contract_updates: HashMap<ContractAddress, ContractUpdate>,
    pub system_contract_updates: HashMap<ContractAddress, SystemContractUpdate>,
    pub declared_cairo_classes: HashSet<ClassHash>,
    pub declared_sierra_classes: HashMap<SierraHash, CasmHash>,
    pub migrated_compiled_classes: HashMap<SierraHash, CasmHash>,
}

#[derive(Default, Debug, Clone, PartialEq, Dummy)]
pub struct StateUpdateData {
    pub contract_updates: HashMap<ContractAddress, ContractUpdate>,
    pub system_contract_updates: HashMap<ContractAddress, SystemContractUpdate>,
    pub declared_cairo_classes: HashSet<ClassHash>,
    pub declared_sierra_classes: HashMap<SierraHash, CasmHash>,
    pub migrated_compiled_classes: HashMap<SierraHash, CasmHash>,
}

#[derive(Default, Debug, Clone, PartialEq, Dummy)]
pub struct ContractUpdate {
    pub storage: HashMap<StorageAddress, StorageValue>,
    /// The class associated with this update as the result of either a deploy
    /// or class replacement transaction.
    pub class: Option<ContractClassUpdate>,
    pub nonce: Option<ContractNonce>,
}

#[derive(Default, Debug, Clone, PartialEq, Dummy)]
pub struct SystemContractUpdate {
    pub storage: HashMap<StorageAddress, StorageValue>,
}

#[derive(Debug, Copy, Clone, PartialEq, Dummy)]
pub enum ContractClassUpdate {
    Deploy(ClassHash),
    Replace(ClassHash),
}

pub struct StateUpdateRef<'a> {
    pub contract_updates: Vec<(&'a ContractAddress, ContractUpdateRef<'a>)>,
    pub system_contract_updates: Vec<(&'a ContractAddress, SystemContractUpdateRef<'a>)>,
    pub declared_sierra_classes: &'a HashMap<SierraHash, CasmHash>,
    pub migrated_compiled_classes: &'a HashMap<SierraHash, CasmHash>,
}

pub struct ContractUpdateRef<'a> {
    pub storage: StorageRef<'a>,
    pub class: &'a Option<ContractClassUpdate>,
    pub nonce: &'a Option<ContractNonce>,
}

pub struct SystemContractUpdateRef<'a> {
    pub storage: StorageRef<'a>,
}

#[derive(Copy, Clone)]
pub enum StorageRef<'a> {
    HashMap(&'a HashMap<StorageAddress, StorageValue>),
    Vec(&'a Vec<(StorageAddress, StorageValue)>),
}

pub enum StorageRefIter<'a> {
    HashMap(hash_map::Iter<'a, StorageAddress, StorageValue>),
    Vec(slice::Iter<'a, (StorageAddress, StorageValue)>),
}

#[derive(Debug, Copy, Clone)]
pub enum FoundStorageValue {
    /// The default zero value for a contract that has been deployed,
    /// but didn't have an explicit storage update at the sought
    /// address.
    Zero,
    /// An explicitly set value (including zero).
    Set(StorageValue),
}

impl ContractUpdate {
    pub fn replaced_class(&self) -> Option<&ClassHash> {
        match &self.class {
            Some(ContractClassUpdate::Replace(hash)) => Some(hash),
            _ => None,
        }
    }

    pub fn deployed_class(&self) -> Option<&ClassHash> {
        match &self.class {
            Some(ContractClassUpdate::Deploy(hash)) => Some(hash),
            _ => None,
        }
    }
}

impl ContractClassUpdate {
    pub fn class_hash(&self) -> ClassHash {
        match self {
            ContractClassUpdate::Deploy(x) => *x,
            ContractClassUpdate::Replace(x) => *x,
        }
    }

    pub fn is_replaced(&self) -> bool {
        matches!(self, ContractClassUpdate::Replace(_))
    }
}

impl StateUpdate {
    pub fn with_block_hash(mut self, block_hash: BlockHash) -> Self {
        self.block_hash = block_hash;
        self
    }

    pub fn with_state_commitment(mut self, state_commitment: StateCommitment) -> Self {
        self.state_commitment = state_commitment;
        self
    }

    pub fn with_parent_state_commitment(
        mut self,
        parent_state_commitment: StateCommitment,
    ) -> Self {
        self.parent_state_commitment = parent_state_commitment;
        self
    }

    pub fn with_contract_nonce(mut self, contract: ContractAddress, nonce: ContractNonce) -> Self {
        self.contract_updates.entry(contract).or_default().nonce = Some(nonce);
        self
    }

    pub fn with_storage_update(
        mut self,
        contract: ContractAddress,
        key: StorageAddress,
        value: StorageValue,
    ) -> Self {
        self.contract_updates
            .entry(contract)
            .or_default()
            .storage
            .insert(key, value);
        self
    }

    pub fn with_system_storage_update(
        mut self,
        contract: ContractAddress,
        key: StorageAddress,
        value: StorageValue,
    ) -> Self {
        self.system_contract_updates
            .entry(contract)
            .or_default()
            .storage
            .insert(key, value);
        self
    }

    pub fn with_deployed_contract(mut self, contract: ContractAddress, class: ClassHash) -> Self {
        self.contract_updates.entry(contract).or_default().class =
            Some(ContractClassUpdate::Deploy(class));
        self
    }

    pub fn with_replaced_class(mut self, contract: ContractAddress, class: ClassHash) -> Self {
        self.contract_updates.entry(contract).or_default().class =
            Some(ContractClassUpdate::Replace(class));
        self
    }

    pub fn with_declared_sierra_class(mut self, sierra: SierraHash, casm: CasmHash) -> Self {
        self.declared_sierra_classes.insert(sierra, casm);
        self
    }

    pub fn with_declared_cairo_class(mut self, cairo: ClassHash) -> Self {
        self.declared_cairo_classes.insert(cairo);
        self
    }

    pub fn with_migrated_compiled_class(mut self, sierra: SierraHash, casm: CasmHash) -> Self {
        self.migrated_compiled_classes.insert(sierra, casm);
        self
    }

    /// The number of individual changes in this state update.
    ///
    /// The total amount of:
    /// - system storage updates
    /// - contract storage updates
    /// - contract nonce updates
    /// - contract deployments
    /// - contract class replacements
    /// - class declarations
    pub fn change_count(&self) -> usize {
        self.declared_cairo_classes.len()
            + self.declared_sierra_classes.len()
            + self
                .system_contract_updates
                .iter()
                .map(|x| x.1.storage.len())
                .sum::<usize>()
            + self
                .contract_updates
                .iter()
                .map(|x| {
                    x.1.storage.len()
                        + x.1.class.as_ref().map(|_| 1).unwrap_or_default()
                        + x.1.nonce.as_ref().map(|_| 1).unwrap_or_default()
                })
                .sum::<usize>()
    }

    /// Returns the contract's new [nonce](ContractNonce) value if it exists in
    /// this state update.
    ///
    /// Note that this will return [Some(ContractNonce::ZERO)] for a contract
    /// that has been deployed, but without an explicit nonce update. This
    /// is consistent with expectations.
    pub fn contract_nonce(&self, contract: ContractAddress) -> Option<ContractNonce> {
        self.contract_updates.get(&contract).and_then(|x| {
            x.nonce.or_else(|| {
                x.class.as_ref().and_then(|c| match c {
                    ContractClassUpdate::Deploy(_) => {
                        // The contract has been just deployed in the pending block, so
                        // its nonce is zero.
                        Some(ContractNonce::ZERO)
                    }
                    ContractClassUpdate::Replace(_) => None,
                })
            })
        })
    }

    /// A contract's new class hash, if it was deployed or replaced in this
    /// state update.
    pub fn contract_class(&self, contract: ContractAddress) -> Option<ClassHash> {
        self.contract_updates
            .get(&contract)
            .and_then(|x| x.class.as_ref().map(|x| x.class_hash()))
    }

    /// Returns true if the class was declared as either a cairo 0 or sierra
    /// class.
    pub fn class_is_declared(&self, class: ClassHash) -> bool {
        if self.declared_cairo_classes.contains(&class) {
            return true;
        }

        self.declared_sierra_classes
            .contains_key(&SierraHash(class.0))
    }

    /// The new storage value if it exists in this state update.
    ///
    /// Note that this will also return the default zero value for a contract
    /// that has been deployed, but without an explicit storage update.
    pub fn storage_value(
        &self,
        contract: ContractAddress,
        key: StorageAddress,
    ) -> Option<StorageValue> {
        self.storage_value_with_provenance(contract, key)
            .map(|found| match found {
                FoundStorageValue::Zero => StorageValue::ZERO,
                FoundStorageValue::Set(inner) => inner,
            })
    }

    pub fn storage_value_with_provenance(
        &self,
        contract: ContractAddress,
        key: StorageAddress,
    ) -> Option<FoundStorageValue> {
        self.contract_updates
            .get(&contract)
            .and_then(|update| {
                update
                    .storage
                    .iter()
                    .find_map(|(k, v)| (k == &key).then_some(FoundStorageValue::Set(*v)))
                    .or_else(|| {
                        update.class.as_ref().and_then(|c| match c {
                            // If the contract has been deployed in pending but the key has not been
                            // set yet return the default value of zero.
                            ContractClassUpdate::Deploy(_) => Some(FoundStorageValue::Zero),
                            ContractClassUpdate::Replace(_) => None,
                        })
                    })
            })
            .or_else(|| {
                self.system_contract_updates
                    .get(&contract)
                    .and_then(|update| {
                        update
                            .storage
                            .iter()
                            .find_map(|(k, v)| (k == &key).then_some(FoundStorageValue::Set(*v)))
                    })
            })
    }

    pub fn compute_state_diff_commitment(&self) -> StateDiffCommitment {
        state_diff_commitment::compute(
            &self.contract_updates,
            &self.system_contract_updates,
            &self.declared_cairo_classes,
            &self.declared_sierra_classes,
            &self.migrated_compiled_classes,
        )
    }

    pub fn state_diff_length(&self) -> u64 {
        let mut len = 0;
        self.contract_updates.iter().for_each(|(_, update)| {
            len += update.storage.len();
            len += usize::from(update.nonce.is_some());
            len += usize::from(update.class.is_some());
        });
        self.system_contract_updates.iter().for_each(|(_, update)| {
            len += update.storage.len();
        });
        len += self.declared_cairo_classes.len()
            + self.declared_sierra_classes.len()
            + self.migrated_compiled_classes.len();
        len.try_into().expect("ptr size is 64bits")
    }

    /// Apply another state update on top of this one.
    pub fn apply(mut self, other: &StateUpdate) -> Self {
        self.block_hash = other.block_hash;
        self.parent_state_commitment = other.parent_state_commitment;
        self.state_commitment = other.state_commitment;

        for (contract, other_update) in &other.contract_updates {
            let update = self.contract_updates.entry(*contract).or_default();

            // Merge storage updates.
            for (key, value) in &other_update.storage {
                update.storage.insert(*key, *value);
            }

            // Merge class updates.
            if let Some(class_update) = &other_update.class {
                update.class = Some(*class_update);
            }

            // Merge nonce updates.
            if let Some(nonce) = other_update.nonce {
                update.nonce = Some(nonce);
            }
        }

        for (contract, other_update) in &other.system_contract_updates {
            let update = self.system_contract_updates.entry(*contract).or_default();

            // Merge storage updates.
            for (key, value) in &other_update.storage {
                update.storage.insert(*key, *value);
            }
        }

        // Merge declared classes.
        self.declared_cairo_classes
            .extend(other.declared_cairo_classes.iter().copied());
        self.declared_sierra_classes
            .extend(other.declared_sierra_classes.iter().map(|(k, v)| (*k, *v)));

        self
    }
}

impl StateUpdateData {
    pub fn compute_state_diff_commitment(&self) -> StateDiffCommitment {
        state_diff_commitment::compute(
            &self.contract_updates,
            &self.system_contract_updates,
            &self.declared_cairo_classes,
            &self.declared_sierra_classes,
            &self.migrated_compiled_classes,
        )
    }

    pub fn is_empty(&self) -> bool {
        self.contract_updates.is_empty()
            && self.system_contract_updates.is_empty()
            && self.declared_cairo_classes.is_empty()
            && self.declared_sierra_classes.is_empty()
    }

    pub fn declared_classes(&self) -> DeclaredClasses {
        DeclaredClasses {
            sierra: self.declared_sierra_classes.clone(),
            cairo: self.declared_cairo_classes.clone(),
        }
    }

    pub fn state_diff_length(&self) -> u64 {
        let mut len = 0;
        self.contract_updates.iter().for_each(|(_, update)| {
            len += update.storage.len();
            len += usize::from(update.nonce.is_some());
            len += usize::from(update.class.is_some());
        });
        self.system_contract_updates.iter().for_each(|(_, update)| {
            len += update.storage.len();
        });
        len += self.declared_cairo_classes.len() + self.declared_sierra_classes.len();
        len.try_into().expect("ptr size is 64bits")
    }

    pub fn as_ref(&self) -> StateUpdateRef<'_> {
        StateUpdateRef::from(self)
    }
}

impl From<StateUpdate> for StateUpdateData {
    fn from(state_update: StateUpdate) -> Self {
        Self {
            contract_updates: state_update.contract_updates,
            system_contract_updates: state_update.system_contract_updates,
            declared_cairo_classes: state_update.declared_cairo_classes,
            declared_sierra_classes: state_update.declared_sierra_classes,
            migrated_compiled_classes: state_update.migrated_compiled_classes,
        }
    }
}

impl<'a> From<&'a StateUpdate> for StateUpdateRef<'a> {
    fn from(state_update: &'a StateUpdate) -> Self {
        Self {
            contract_updates: state_update
                .contract_updates
                .iter()
                .map(|(k, v)| {
                    (
                        k,
                        ContractUpdateRef {
                            storage: StorageRef::HashMap(&v.storage),
                            class: &v.class,
                            nonce: &v.nonce,
                        },
                    )
                })
                .collect(),
            system_contract_updates: state_update
                .system_contract_updates
                .iter()
                .map(|(k, v)| {
                    (
                        k,
                        SystemContractUpdateRef {
                            storage: StorageRef::HashMap(&v.storage),
                        },
                    )
                })
                .collect(),
            declared_sierra_classes: &state_update.declared_sierra_classes,
            migrated_compiled_classes: &state_update.migrated_compiled_classes,
        }
    }
}

impl<'a> From<&'a mut StateUpdate> for StateUpdateRef<'a> {
    fn from(state_update: &'a mut StateUpdate) -> Self {
        Self::from(state_update as &'a StateUpdate)
    }
}

impl<'a> From<&'a StateUpdateData> for StateUpdateRef<'a> {
    fn from(state_update: &'a StateUpdateData) -> Self {
        Self {
            contract_updates: state_update
                .contract_updates
                .iter()
                .map(|(k, v)| {
                    (
                        k,
                        ContractUpdateRef {
                            storage: StorageRef::HashMap(&v.storage),
                            class: &v.class,
                            nonce: &v.nonce,
                        },
                    )
                })
                .collect(),
            system_contract_updates: state_update
                .system_contract_updates
                .iter()
                .map(|(k, v)| {
                    (
                        k,
                        SystemContractUpdateRef {
                            storage: StorageRef::HashMap(&v.storage),
                        },
                    )
                })
                .collect(),
            declared_sierra_classes: &state_update.declared_sierra_classes,
            migrated_compiled_classes: &state_update.migrated_compiled_classes,
        }
    }
}

impl<'a> From<&'a mut StateUpdateData> for StateUpdateRef<'a> {
    fn from(state_update: &'a mut StateUpdateData) -> Self {
        Self::from(state_update as &'a StateUpdateData)
    }
}

impl StorageRef<'_> {
    pub fn iter(&self) -> StorageRefIter<'_> {
        match self {
            StorageRef::HashMap(map) => StorageRefIter::HashMap(map.iter()),
            StorageRef::Vec(vec) => StorageRefIter::Vec(vec.iter()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            StorageRef::HashMap(map) => map.is_empty(),
            StorageRef::Vec(vec) => vec.is_empty(),
        }
    }
}

impl<'a> From<&'a ContractUpdate> for ContractUpdateRef<'a> {
    fn from(x: &'a ContractUpdate) -> Self {
        ContractUpdateRef {
            storage: (&x.storage).into(),
            class: &x.class,
            nonce: &x.nonce,
        }
    }
}

impl<'a> From<&'a SystemContractUpdate> for SystemContractUpdateRef<'a> {
    fn from(x: &'a SystemContractUpdate) -> Self {
        SystemContractUpdateRef {
            storage: (&x.storage).into(),
        }
    }
}

impl<'a> From<&'a HashMap<StorageAddress, StorageValue>> for StorageRef<'a> {
    fn from(x: &'a HashMap<StorageAddress, StorageValue>) -> Self {
        StorageRef::HashMap(x)
    }
}

impl<'a> From<&'a Vec<(StorageAddress, StorageValue)>> for StorageRef<'a> {
    fn from(x: &'a Vec<(StorageAddress, StorageValue)>) -> Self {
        StorageRef::Vec(x)
    }
}

impl<'a> IntoIterator for &'a StorageRef<'a> {
    type Item = (&'a StorageAddress, &'a StorageValue);
    type IntoIter = StorageRefIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> Iterator for StorageRefIter<'a> {
    type Item = (&'a StorageAddress, &'a StorageValue);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StorageRefIter::HashMap(iter) => iter.next(),
            StorageRefIter::Vec(iter) => iter.next().map(|(k, v)| (k, v)),
        }
    }
}

mod state_diff_commitment {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use pathfinder_crypto::hash::PoseidonHasher;
    use pathfinder_crypto::MontFelt;

    use super::{ContractUpdate, SystemContractUpdate};
    use crate::{
        felt_bytes,
        CasmHash,
        ClassHash,
        ContractAddress,
        SierraHash,
        StateDiffCommitment,
    };

    /// Compute the state diff commitment used in block commitment signatures.
    ///
    /// How to compute the value is documented in the [Starknet documentation](https://docs.starknet.io/learn/protocol/blocks#state-diff-commitment).
    pub fn compute(
        contract_updates: &HashMap<ContractAddress, ContractUpdate>,
        system_contract_updates: &HashMap<ContractAddress, SystemContractUpdate>,
        declared_cairo_classes: &HashSet<ClassHash>,
        declared_sierra_classes: &HashMap<SierraHash, CasmHash>,
        migrated_compiled_classes: &HashMap<SierraHash, CasmHash>,
    ) -> StateDiffCommitment {
        let mut hasher = PoseidonHasher::new();
        hasher.write(felt_bytes!(b"STARKNET_STATE_DIFF0").into());
        // Hash the deployed contracts.
        let deployed_contracts: BTreeMap<_, _> = contract_updates
            .iter()
            .filter_map(|(address, update)| {
                update
                    .class
                    .as_ref()
                    .map(|update| (*address, update.class_hash()))
            })
            .collect();
        hasher.write(MontFelt::from(deployed_contracts.len() as u64));
        for (address, class_hash) in deployed_contracts {
            hasher.write(MontFelt::from(address.0));
            hasher.write(MontFelt::from(class_hash.0));
        }
        // Hash the declared classes and the migrated compiled classes.
        let declared_classes: BTreeSet<_> = declared_sierra_classes
            .iter()
            .chain(migrated_compiled_classes.iter())
            .map(|(sierra, casm)| (*sierra, *casm))
            .collect();
        hasher.write(MontFelt::from(declared_classes.len() as u64));
        for (sierra, casm) in declared_classes {
            hasher.write(MontFelt::from(sierra.0));
            hasher.write(MontFelt::from(casm.0));
        }
        // Hash the old declared classes.
        let deprecated_declared_classes: BTreeSet<_> =
            declared_cairo_classes.iter().copied().collect();
        hasher.write(MontFelt::from(deprecated_declared_classes.len() as u64));
        for class_hash in deprecated_declared_classes {
            hasher.write(MontFelt::from(class_hash.0));
        }
        hasher.write(MontFelt::ONE);
        hasher.write(MontFelt::ZERO);
        // Hash the storage diffs.
        let storage_diffs: BTreeMap<_, _> = contract_updates
            .iter()
            .map(|(address, update)| (address, &update.storage))
            .chain(
                system_contract_updates
                    .iter()
                    .map(|(address, update)| (address, &update.storage)),
            )
            .filter_map(|(address, storage)| {
                if storage.is_empty() {
                    None
                } else {
                    let updates: BTreeMap<_, _> =
                        storage.iter().map(|(key, value)| (*key, *value)).collect();
                    Some((*address, updates))
                }
            })
            .collect();
        hasher.write(MontFelt::from(storage_diffs.len() as u64));
        for (address, updates) in storage_diffs {
            hasher.write(MontFelt::from(address.0));
            hasher.write(MontFelt::from(updates.len() as u64));
            for (key, value) in updates {
                hasher.write(MontFelt::from(key.0));
                hasher.write(MontFelt::from(value.0));
            }
        }
        // Hash the nonce updates.
        let nonces: BTreeMap<_, _> = contract_updates
            .iter()
            .filter_map(|(address, update)| update.nonce.map(|nonce| (*address, nonce)))
            .collect();
        hasher.write(MontFelt::from(nonces.len() as u64));
        for (address, nonce) in nonces {
            hasher.write(MontFelt::from(address.0));
            hasher.write(MontFelt::from(nonce.0));
        }
        StateDiffCommitment(hasher.finish().into())
    }
}

#[derive(Debug, PartialEq)]
pub enum ReverseContractUpdate {
    Deleted,
    Updated(ContractUpdate),
}

impl ReverseContractUpdate {
    pub fn update_mut(&mut self) -> Option<&mut ContractUpdate> {
        match self {
            Self::Deleted => None,
            Self::Updated(update) => Some(update),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeclaredClasses {
    pub sierra: HashMap<SierraHash, CasmHash>,
    pub cairo: HashSet<ClassHash>,
}

impl DeclaredClasses {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.sierra.len() + self.cairo.len()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateUpdateError {
    #[error("Contract class hash missing for contract {0}")]
    ContractClassHashMissing(ContractAddress),
    #[error(transparent)]
    StorageError(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macro_prelude::*;

    #[test]
    fn change_count() {
        let state_update = StateUpdate::default()
            .with_contract_nonce(contract_address!("0x1"), contract_nonce!("0x2"))
            .with_contract_nonce(contract_address!("0x4"), contract_nonce!("0x5"))
            .with_declared_cairo_class(class_hash!("0x3"))
            .with_declared_sierra_class(sierra_hash!("0x4"), casm_hash!("0x5"))
            .with_deployed_contract(contract_address!("0x1"), class_hash!("0x3"))
            .with_replaced_class(contract_address!("0x33"), class_hash!("0x35"))
            .with_system_storage_update(
                ContractAddress::ONE,
                storage_address!("0x10"),
                storage_value!("0x99"),
            )
            .with_storage_update(
                contract_address!("0x33"),
                storage_address!("0x10"),
                storage_value!("0x99"),
            );

        assert_eq!(state_update.change_count(), 8);
    }

    #[test]
    fn contract_nonce() {
        let state_update = StateUpdate::default()
            .with_contract_nonce(contract_address!("0x1"), contract_nonce!("0x2"))
            .with_deployed_contract(contract_address!("0x2"), class_hash!("0x4"))
            .with_contract_nonce(contract_address!("0x10"), contract_nonce!("0x20"))
            .with_deployed_contract(contract_address!("0x10"), class_hash!("0x12"))
            .with_replaced_class(contract_address!("0x123"), class_hash!("0x1244"))
            .with_replaced_class(contract_address!("0x1234"), class_hash!("0x12445"))
            .with_contract_nonce(contract_address!("0x1234"), contract_nonce!("0x1111"));

        assert!(state_update
            .contract_nonce(contract_address_bytes!(b"not present"))
            .is_none());

        let result = state_update.contract_nonce(contract_address!("0x1"));
        assert_eq!(result, Some(contract_nonce!("0x2")));

        // A newly deployed contract with an explicit nonce set.
        let result = state_update.contract_nonce(contract_address!("0x10"));
        assert_eq!(result, Some(contract_nonce!("0x20")));

        // A newly deployed contract without an explicit nonce set should be zero
        let result = state_update.contract_nonce(contract_address!("0x2"));
        assert_eq!(result, Some(ContractNonce::ZERO));

        // A replaced contract with an explicit nonce set.
        let result = state_update.contract_nonce(contract_address!("0x1234"));
        assert_eq!(result, Some(contract_nonce!("0x1111")));

        // A replaced class without an explicit nonce.
        assert!(state_update
            .contract_nonce(contract_address!("0x123"))
            .is_none());
    }

    mod storage_value {
        use super::*;

        #[test]
        fn set() {
            let c = contract_address!("0x1");
            let k = storage_address!("0x2");
            let v = storage_value!("0x3");
            let state_update = StateUpdate::default().with_storage_update(c, k, v);
            let result = state_update.storage_value(c, k);
            assert_eq!(result, Some(v))
        }

        #[test]
        fn not_set() {
            let c = contract_address!("0x1");
            let k = storage_address!("0x2");
            let v = storage_value!("0x3");
            let state_update = StateUpdate::default().with_storage_update(c, k, v);
            let result = state_update.storage_value(contract_address!("0x4"), k);
            assert!(result.is_none());

            let result = state_update.storage_value(c, storage_address!("0x24"));
            assert!(result.is_none());
        }

        #[test]
        fn deployed_and_not_set() {
            let c = contract_address!("0x1");
            let state_update = StateUpdate::default().with_deployed_contract(c, class_hash!("0x1"));
            let result = state_update.storage_value(c, storage_address!("0x2"));
            assert_eq!(result, Some(StorageValue::ZERO));
        }

        #[test]
        fn deployed_and_set() {
            let c = contract_address!("0x1");
            let k = storage_address!("0x2");
            let v = storage_value!("0x3");
            let state_update = StateUpdate::default()
                .with_deployed_contract(c, class_hash!("0x1"))
                .with_storage_update(c, k, v);
            let result = state_update.storage_value(c, k);
            assert_eq!(result, Some(v));
        }

        #[test]
        fn replaced_and_not_set() {
            let c = contract_address!("0x1");
            let state_update = StateUpdate::default().with_replaced_class(c, class_hash!("0x1"));
            let result = state_update.storage_value(c, storage_address!("0x2"));
            assert!(result.is_none());
        }

        #[test]
        fn replaced_and_set() {
            let c = contract_address!("0x1");
            let k = storage_address!("0x2");
            let v = storage_value!("0x3");
            let state_update = StateUpdate::default()
                .with_replaced_class(c, class_hash!("0x1"))
                .with_storage_update(c, k, v);
            let result = state_update.storage_value(c, k);
            assert_eq!(result, Some(v));
        }

        #[test]
        fn system_contract_and_set() {
            let c = contract_address!("0x1");
            let k = storage_address!("0x2");
            let v = storage_value!("0x3");
            let state_update = StateUpdate::default().with_system_storage_update(c, k, v);
            let result = state_update.storage_value(c, k);
            assert_eq!(result, Some(v))
        }

        #[test]
        fn system_contract_and_not_set() {
            let c = contract_address!("0x1");
            let k = storage_address!("0x2");
            let v = storage_value!("0x3");
            let state_update = StateUpdate::default().with_system_storage_update(c, k, v);
            let result = state_update.storage_value(contract_address!("0x4"), k);
            assert_eq!(result, None);
            let result = state_update.storage_value(c, storage_address!("0x24"));
            assert_eq!(result, None);
        }
    }

    #[test]
    fn class_is_declared() {
        let cairo = class_hash_bytes!(b"cairo class");
        let sierra = class_hash_bytes!(b"sierra class");

        let state_update = StateUpdate::default()
            .with_declared_cairo_class(cairo)
            .with_declared_sierra_class(SierraHash(sierra.0), casm_hash_bytes!(b"anything"));

        assert!(state_update.class_is_declared(cairo));
        assert!(state_update.class_is_declared(sierra));
        assert!(!state_update.class_is_declared(class_hash_bytes!(b"nope")));
    }

    #[test]
    fn contract_class() {
        let deployed = contract_address_bytes!(b"deployed");
        let deployed_class = class_hash_bytes!(b"deployed class");
        let replaced = contract_address_bytes!(b"replaced");
        let replaced_class = class_hash_bytes!(b"replaced class");

        let state_update = StateUpdate::default()
            .with_deployed_contract(deployed, deployed_class)
            .with_replaced_class(replaced, replaced_class);

        let result = state_update.contract_class(deployed);
        assert_eq!(result, Some(deployed_class));

        let result = state_update.contract_class(replaced);
        assert_eq!(result, Some(replaced_class));

        assert!(state_update
            .contract_class(contract_address_bytes!(b"bogus"))
            .is_none());
    }

    /// Source:
    /// https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/state_diff_hash_test.rs#L14
    #[test]
    fn test_0_13_2_state_diff_commitment() {
        let contract_updates: HashMap<_, _> = [
            (
                ContractAddress(0u64.into()),
                ContractUpdate {
                    class: Some(ContractClassUpdate::Deploy(ClassHash(1u64.into()))),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(2u64.into()),
                ContractUpdate {
                    class: Some(ContractClassUpdate::Deploy(ClassHash(3u64.into()))),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(4u64.into()),
                ContractUpdate {
                    storage: [
                        (StorageAddress(5u64.into()), StorageValue(6u64.into())),
                        (StorageAddress(7u64.into()), StorageValue(8u64.into())),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(9u64.into()),
                ContractUpdate {
                    storage: [(StorageAddress(10u64.into()), StorageValue(11u64.into()))]
                        .iter()
                        .cloned()
                        .collect(),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(17u64.into()),
                ContractUpdate {
                    nonce: Some(ContractNonce(18u64.into())),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(19u64.into()),
                ContractUpdate {
                    class: Some(ContractClassUpdate::Replace(ClassHash(20u64.into()))),
                    ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect();
        let declared_sierra_classes: HashMap<_, _> = [
            (SierraHash(12u64.into()), CasmHash(13u64.into())),
            (SierraHash(14u64.into()), CasmHash(15u64.into())),
        ]
        .iter()
        .cloned()
        .collect();
        let declared_cairo_classes: HashSet<_> =
            [ClassHash(16u64.into())].iter().cloned().collect();

        let expected_hash = StateDiffCommitment(felt!(
            "0x0281f5966e49ad7dad9323826d53d1d27c0c4e6ebe5525e2e2fbca549bfa0a67"
        ));

        assert_eq!(
            expected_hash,
            state_diff_commitment::compute(
                &contract_updates,
                &Default::default(),
                &declared_cairo_classes,
                &declared_sierra_classes,
                &Default::default(),
            )
        );
    }

    /// Source:
    /// https://github.com/starkware-libs/starknet-api/blob/5565e5282f5fead364a41e49c173940fd83dee00/src/block_hash/state_diff_hash_test.rs#L14
    #[test]
    fn test_0_13_2_state_diff_commitment_with_migrated_compiled_classes() {
        let contract_updates: HashMap<_, _> = [
            (
                ContractAddress(0u64.into()),
                ContractUpdate {
                    class: Some(ContractClassUpdate::Deploy(ClassHash(1u64.into()))),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(2u64.into()),
                ContractUpdate {
                    class: Some(ContractClassUpdate::Deploy(ClassHash(3u64.into()))),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(4u64.into()),
                ContractUpdate {
                    storage: [
                        (StorageAddress(5u64.into()), StorageValue(6u64.into())),
                        (StorageAddress(7u64.into()), StorageValue(8u64.into())),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(9u64.into()),
                ContractUpdate {
                    storage: [(StorageAddress(10u64.into()), StorageValue(11u64.into()))]
                        .iter()
                        .cloned()
                        .collect(),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(17u64.into()),
                ContractUpdate {
                    nonce: Some(ContractNonce(18u64.into())),
                    ..Default::default()
                },
            ),
            (
                ContractAddress(19u64.into()),
                ContractUpdate {
                    class: Some(ContractClassUpdate::Replace(ClassHash(20u64.into()))),
                    ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect();
        let declared_sierra_classes: HashMap<_, _> =
            [(SierraHash(12u64.into()), CasmHash(13u64.into()))]
                .iter()
                .cloned()
                .collect();
        let migrated_compiled_classes: HashMap<_, _> =
            [(SierraHash(14u64.into()), CasmHash(15u64.into()))]
                .iter()
                .cloned()
                .collect();
        let declared_cairo_classes: HashSet<_> =
            [ClassHash(16u64.into())].iter().cloned().collect();

        let expected_hash = StateDiffCommitment(felt!(
            "0x0281f5966e49ad7dad9323826d53d1d27c0c4e6ebe5525e2e2fbca549bfa0a67"
        ));

        assert_eq!(
            expected_hash,
            state_diff_commitment::compute(
                &contract_updates,
                &Default::default(),
                &declared_cairo_classes,
                &declared_sierra_classes,
                &migrated_compiled_classes,
            )
        );
    }

    #[test]
    fn apply() {
        let state_update = StateUpdate::default()
            .with_contract_nonce(contract_address!("0x1"), contract_nonce!("0x2"))
            .with_contract_nonce(contract_address!("0x4"), contract_nonce!("0x5"))
            .with_declared_cairo_class(class_hash!("0x3"))
            .with_declared_sierra_class(sierra_hash!("0x4"), casm_hash!("0x5"))
            .with_deployed_contract(contract_address!("0x1"), class_hash!("0x3"))
            .with_replaced_class(contract_address!("0x33"), class_hash!("0x35"))
            .with_system_storage_update(
                ContractAddress::ONE,
                storage_address!("0x10"),
                storage_value!("0x99"),
            )
            .with_storage_update(
                contract_address!("0x33"),
                storage_address!("0x10"),
                storage_value!("0x99"),
            );

        let second_state_update = StateUpdate::default()
            .with_contract_nonce(contract_address!("0x1"), contract_nonce!("0x3"))
            .with_contract_nonce(contract_address!("0x5"), contract_nonce!("0x5"))
            .with_declared_cairo_class(class_hash!("0x6"))
            .with_declared_sierra_class(sierra_hash!("0x7"), casm_hash!("0x8"))
            .with_deployed_contract(contract_address!("0x9"), class_hash!("0x7"))
            .with_replaced_class(contract_address!("0x33"), class_hash!("0x37"))
            .with_system_storage_update(
                ContractAddress::ONE,
                storage_address!("0x11"),
                storage_value!("0x100"),
            )
            .with_storage_update(
                contract_address!("0x33"),
                storage_address!("0x10"),
                storage_value!("0x100"),
            );

        let combined = state_update.apply(&second_state_update);
        let expected = StateUpdate::default()
            .with_contract_nonce(contract_address!("0x1"), contract_nonce!("0x3"))
            .with_contract_nonce(contract_address!("0x4"), contract_nonce!("0x5"))
            .with_contract_nonce(contract_address!("0x5"), contract_nonce!("0x5"))
            .with_declared_cairo_class(class_hash!("0x3"))
            .with_declared_cairo_class(class_hash!("0x6"))
            .with_declared_sierra_class(sierra_hash!("0x4"), casm_hash!("0x5"))
            .with_declared_sierra_class(sierra_hash!("0x7"), casm_hash!("0x8"))
            .with_deployed_contract(contract_address!("0x1"), class_hash!("0x3"))
            .with_deployed_contract(contract_address!("0x9"), class_hash!("0x7"))
            .with_replaced_class(contract_address!("0x33"), class_hash!("0x37"))
            .with_system_storage_update(
                ContractAddress::ONE,
                storage_address!("0x10"),
                storage_value!("0x99"),
            )
            .with_system_storage_update(
                ContractAddress::ONE,
                storage_address!("0x11"),
                storage_value!("0x100"),
            )
            .with_storage_update(
                contract_address!("0x33"),
                storage_address!("0x10"),
                storage_value!("0x100"),
            );
        assert_eq!(combined, expected);
    }
}
