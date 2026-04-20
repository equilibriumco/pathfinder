use fake::{Dummy, Fake, Faker};
use pathfinder_crypto::hash::{HashChain as PedersenHasher, PoseidonHasher};
use pathfinder_crypto::Felt;
use primitive_types::H256;

use crate::prelude::*;
use crate::{
    felt_bytes,
    AccountDeploymentDataElem,
    PaymasterDataElem,
    ProofFactElem,
    ResourceAmount,
    ResourcePricePerUnit,
    Tip,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Dummy)]
pub struct Transaction {
    pub hash: TransactionHash,
    pub variant: TransactionVariant,
}

impl Transaction {
    /// Verifies the transaction hash against the transaction data.
    #[must_use = "Should act on verification result"]
    pub fn verify_hash(&self, chain_id: ChainId) -> bool {
        self.variant.verify_hash(chain_id, self.hash)
    }

    pub fn version(&self) -> TransactionVersion {
        match &self.variant {
            TransactionVariant::DeclareV0(_) => TransactionVersion::ZERO,
            TransactionVariant::DeclareV1(_) => TransactionVersion::ONE,
            TransactionVariant::DeclareV2(_) => TransactionVersion::TWO,
            TransactionVariant::DeclareV3(_) => TransactionVersion::THREE,
            TransactionVariant::DeployV0(_) => TransactionVersion::ZERO,
            TransactionVariant::DeployV1(_) => TransactionVersion::ONE,
            TransactionVariant::DeployAccountV1(_) => TransactionVersion::ONE,
            TransactionVariant::DeployAccountV3(_) => TransactionVersion::THREE,
            TransactionVariant::InvokeV0(_) => TransactionVersion::ZERO,
            TransactionVariant::InvokeV1(_) => TransactionVersion::ONE,
            TransactionVariant::InvokeV3(_) => TransactionVersion::THREE,
            TransactionVariant::L1Handler(_) => TransactionVersion::ZERO,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Dummy)]
pub enum TransactionVariant {
    DeclareV0(DeclareTransactionV0V1),
    DeclareV1(DeclareTransactionV0V1),
    DeclareV2(DeclareTransactionV2),
    DeclareV3(DeclareTransactionV3),
    DeployV0(DeployTransactionV0),
    DeployV1(DeployTransactionV1),
    DeployAccountV1(DeployAccountTransactionV1),
    DeployAccountV3(DeployAccountTransactionV3),
    InvokeV0(InvokeTransactionV0),
    InvokeV1(InvokeTransactionV1),
    InvokeV3(InvokeTransactionV3),
    L1Handler(L1HandlerTransaction),
}

impl Default for TransactionVariant {
    fn default() -> Self {
        Self::DeclareV0(Default::default())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionKind {
    Declare,
    Deploy,
    DeployAccount,
    Invoke,
    L1Handler,
}

impl TransactionVariant {
    #[must_use = "Should act on verification result"]
    fn verify_hash(&self, chain_id: ChainId, expected: TransactionHash) -> bool {
        if expected == self.calculate_hash(chain_id, false) {
            return true;
        }

        // Some transaction variants had a different hash calculation in ancient times.
        if Some(expected) == self.calculate_legacy_hash(chain_id) {
            return true;
        }

        // L1 Handlers had a specific hash calculation for Starknet v0.7 blocks.
        if let Self::L1Handler(l1_handler) = self {
            if expected == l1_handler.calculate_v07_hash(chain_id) {
                return true;
            }
        }

        false
    }

    pub fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        match self {
            TransactionVariant::DeclareV0(tx) => tx.calculate_hash_v0(chain_id, query_only),
            TransactionVariant::DeclareV1(tx) => tx.calculate_hash_v1(chain_id, query_only),
            TransactionVariant::DeclareV2(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::DeclareV3(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::DeployV0(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::DeployV1(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::DeployAccountV1(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::DeployAccountV3(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::InvokeV0(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::InvokeV1(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::InvokeV3(tx) => tx.calculate_hash(chain_id, query_only),
            TransactionVariant::L1Handler(tx) => tx.calculate_hash(chain_id),
        }
    }

    pub fn kind(&self) -> TransactionKind {
        match self {
            TransactionVariant::DeclareV0(_) => TransactionKind::Declare,
            TransactionVariant::DeclareV1(_) => TransactionKind::Declare,
            TransactionVariant::DeclareV2(_) => TransactionKind::Declare,
            TransactionVariant::DeclareV3(_) => TransactionKind::Declare,
            TransactionVariant::DeployV0(_) => TransactionKind::Deploy,
            TransactionVariant::DeployV1(_) => TransactionKind::Deploy,
            TransactionVariant::DeployAccountV1(_) => TransactionKind::DeployAccount,
            TransactionVariant::DeployAccountV3(_) => TransactionKind::DeployAccount,
            TransactionVariant::InvokeV0(_) => TransactionKind::Invoke,
            TransactionVariant::InvokeV1(_) => TransactionKind::Invoke,
            TransactionVariant::InvokeV3(_) => TransactionKind::Invoke,
            TransactionVariant::L1Handler(_) => TransactionKind::L1Handler,
        }
    }

    /// Some variants had a different hash calculations for blocks around
    /// Starknet v0.8 and earlier. The hash excluded the transaction version
    /// and nonce.
    fn calculate_legacy_hash(&self, chain_id: ChainId) -> Option<TransactionHash> {
        let hash = match self {
            TransactionVariant::DeployV0(tx) => tx.calculate_legacy_hash(chain_id),
            TransactionVariant::DeployV1(tx) => tx.calculate_legacy_hash(chain_id),
            TransactionVariant::InvokeV0(tx) => tx.calculate_legacy_hash(chain_id),
            TransactionVariant::L1Handler(tx) => tx.calculate_legacy_hash(chain_id),
            _ => return None,
        };

        Some(hash)
    }

    /// Compute contract address for deploy and deploy account transactions. Do
    /// nothing in case of other transactions.
    ///
    /// ### Important
    ///
    /// This function should not be called in async context.
    pub fn calculate_contract_address(&mut self) {
        match self {
            TransactionVariant::DeployV0(DeployTransactionV0 {
                class_hash,
                constructor_calldata,
                contract_address,
                contract_address_salt,
            })
            | TransactionVariant::DeployV1(DeployTransactionV1 {
                class_hash,
                constructor_calldata,
                contract_address,
                contract_address_salt,
            }) => {
                *contract_address = ContractAddress::deployed_contract_address(
                    constructor_calldata.iter().map(|d| CallParam(d.0)),
                    contract_address_salt,
                    class_hash,
                );
            }
            TransactionVariant::DeployAccountV1(DeployAccountTransactionV1 {
                class_hash,
                constructor_calldata,
                contract_address,
                contract_address_salt,
                ..
            })
            | TransactionVariant::DeployAccountV3(DeployAccountTransactionV3 {
                class_hash,
                constructor_calldata,
                contract_address,
                contract_address_salt,
                ..
            }) => {
                *contract_address = ContractAddress::deployed_contract_address(
                    constructor_calldata.iter().copied(),
                    contract_address_salt,
                    class_hash,
                );
            }
            _ => {}
        }
    }

    pub fn sender_address(&self) -> ContractAddress {
        match self {
            TransactionVariant::DeclareV0(tx) => tx.sender_address,
            TransactionVariant::DeclareV1(tx) => tx.sender_address,
            TransactionVariant::DeclareV2(tx) => tx.sender_address,
            TransactionVariant::DeclareV3(tx) => tx.sender_address,
            TransactionVariant::DeployV0(tx) => tx.contract_address,
            TransactionVariant::DeployV1(tx) => tx.contract_address,
            TransactionVariant::DeployAccountV1(tx) => tx.contract_address,
            TransactionVariant::DeployAccountV3(tx) => tx.contract_address,
            TransactionVariant::InvokeV0(tx) => tx.sender_address,
            TransactionVariant::InvokeV1(tx) => tx.sender_address,
            TransactionVariant::InvokeV3(tx) => tx.sender_address,
            TransactionVariant::L1Handler(tx) => tx.contract_address,
        }
    }
}

impl From<DeclareTransactionV2> for TransactionVariant {
    fn from(value: DeclareTransactionV2) -> Self {
        Self::DeclareV2(value)
    }
}
impl From<DeclareTransactionV3> for TransactionVariant {
    fn from(value: DeclareTransactionV3) -> Self {
        Self::DeclareV3(value)
    }
}
impl From<DeployTransactionV0> for TransactionVariant {
    fn from(value: DeployTransactionV0) -> Self {
        Self::DeployV0(value)
    }
}
impl From<DeployTransactionV1> for TransactionVariant {
    fn from(value: DeployTransactionV1) -> Self {
        Self::DeployV1(value)
    }
}
impl From<DeployAccountTransactionV1> for TransactionVariant {
    fn from(value: DeployAccountTransactionV1) -> Self {
        Self::DeployAccountV1(value)
    }
}
impl From<DeployAccountTransactionV3> for TransactionVariant {
    fn from(value: DeployAccountTransactionV3) -> Self {
        Self::DeployAccountV3(value)
    }
}
impl From<InvokeTransactionV0> for TransactionVariant {
    fn from(value: InvokeTransactionV0) -> Self {
        Self::InvokeV0(value)
    }
}
impl From<InvokeTransactionV1> for TransactionVariant {
    fn from(value: InvokeTransactionV1) -> Self {
        Self::InvokeV1(value)
    }
}
impl From<InvokeTransactionV3> for TransactionVariant {
    fn from(value: InvokeTransactionV3) -> Self {
        Self::InvokeV3(value)
    }
}
impl From<L1HandlerTransaction> for TransactionVariant {
    fn from(value: L1HandlerTransaction) -> Self {
        Self::L1Handler(value)
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct DeclareTransactionV0V1 {
    pub class_hash: ClassHash,
    pub max_fee: Fee,
    pub nonce: TransactionNonce,
    pub signature: Vec<TransactionSignatureElem>,
    pub sender_address: ContractAddress,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Dummy)]
pub struct DeclareTransactionV2 {
    pub class_hash: ClassHash,
    pub max_fee: Fee,
    pub nonce: TransactionNonce,
    pub signature: Vec<TransactionSignatureElem>,
    pub sender_address: ContractAddress,
    pub compiled_class_hash: CasmHash,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Dummy)]
pub struct DeclareTransactionV3 {
    pub class_hash: ClassHash,
    pub nonce: TransactionNonce,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBounds,
    pub tip: Tip,
    pub paymaster_data: Vec<PaymasterDataElem>,
    pub signature: Vec<TransactionSignatureElem>,
    pub account_deployment_data: Vec<AccountDeploymentDataElem>,
    pub sender_address: ContractAddress,
    pub compiled_class_hash: CasmHash,
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct DeployTransactionV0 {
    pub class_hash: ClassHash,
    pub contract_address: ContractAddress,
    pub contract_address_salt: ContractAddressSalt,
    pub constructor_calldata: Vec<ConstructorParam>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct DeployTransactionV1 {
    pub class_hash: ClassHash,
    pub contract_address: ContractAddress,
    pub contract_address_salt: ContractAddressSalt,
    pub constructor_calldata: Vec<ConstructorParam>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct DeployAccountTransactionV1 {
    pub contract_address: ContractAddress,
    pub max_fee: Fee,
    pub signature: Vec<TransactionSignatureElem>,
    pub nonce: TransactionNonce,
    pub contract_address_salt: ContractAddressSalt,
    pub constructor_calldata: Vec<CallParam>,
    pub class_hash: ClassHash,
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct DeployAccountTransactionV3 {
    pub contract_address: ContractAddress,
    pub signature: Vec<TransactionSignatureElem>,
    pub nonce: TransactionNonce,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBounds,
    pub tip: Tip,
    pub paymaster_data: Vec<PaymasterDataElem>,
    pub contract_address_salt: ContractAddressSalt,
    pub constructor_calldata: Vec<CallParam>,
    pub class_hash: ClassHash,
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct InvokeTransactionV0 {
    pub calldata: Vec<CallParam>,
    pub sender_address: ContractAddress,
    pub entry_point_selector: EntryPoint,
    pub entry_point_type: Option<EntryPointType>,
    pub max_fee: Fee,
    pub signature: Vec<TransactionSignatureElem>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Dummy)]
pub struct InvokeTransactionV1 {
    pub calldata: Vec<CallParam>,
    pub sender_address: ContractAddress,
    pub max_fee: Fee,
    pub signature: Vec<TransactionSignatureElem>,
    pub nonce: TransactionNonce,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Dummy)]
pub struct InvokeTransactionV3 {
    pub signature: Vec<TransactionSignatureElem>,
    pub nonce: TransactionNonce,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBounds,
    pub tip: Tip,
    pub paymaster_data: Vec<PaymasterDataElem>,
    pub account_deployment_data: Vec<AccountDeploymentDataElem>,
    pub calldata: Vec<CallParam>,
    pub sender_address: ContractAddress,
    pub proof_facts: Vec<ProofFactElem>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Dummy)]
pub struct L1HandlerTransaction {
    pub contract_address: ContractAddress,
    pub entry_point_selector: EntryPoint,
    pub nonce: TransactionNonce,
    pub calldata: Vec<CallParam>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Dummy)]
pub enum EntryPointType {
    External,
    L1Handler,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Dummy)]
pub struct ResourceBounds {
    pub l1_gas: ResourceBound,
    pub l2_gas: ResourceBound,
    pub l1_data_gas: Option<ResourceBound>,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Dummy)]
pub struct ResourceBound {
    pub max_amount: ResourceAmount,
    pub max_price_per_unit: ResourcePricePerUnit,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Dummy)]
pub enum DataAvailabilityMode {
    #[default]
    L1,
    L2,
}

impl From<DataAvailabilityMode> for u64 {
    fn from(value: DataAvailabilityMode) -> Self {
        match value {
            DataAvailabilityMode::L1 => 0,
            DataAvailabilityMode::L2 => 1,
        }
    }
}

impl DeclareTransactionV0V1 {
    fn calculate_hash_v0(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"declare"),
            version: TransactionVersion::ZERO.with_query_only(query_only),
            address: self.sender_address,
            data_hash: PedersenHasher::default().finalize(),
            nonce_or_class: Some(self.class_hash.0),
            ..Default::default()
        }
        .hash(chain_id)
    }

    fn calculate_hash_v1(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"declare"),
            version: TransactionVersion::ONE.with_query_only(query_only),
            address: self.sender_address,
            data_hash: PedersenHasher::single(self.class_hash.0),
            max_fee: self.max_fee,
            nonce_or_class: Some(self.nonce.0),
            ..Default::default()
        }
        .hash(chain_id)
    }
}

impl DeclareTransactionV2 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"declare"),
            version: TransactionVersion::TWO.with_query_only(query_only),
            address: self.sender_address,
            data_hash: PedersenHasher::single(self.class_hash.0),
            max_fee: self.max_fee,
            nonce_or_class: Some(self.nonce.0),
            casm_hash: Some(self.compiled_class_hash),
            ..Default::default()
        }
        .hash(chain_id)
    }
}

impl DeployTransactionV0 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"deploy"),
            version: TransactionVersion::ZERO.with_query_only(query_only),
            address: self.contract_address,
            entry_point: EntryPoint::CONSTRUCTOR,
            data_hash: self.constructor_calldata_hash(),
            ..Default::default()
        }
        .hash(chain_id)
    }

    fn calculate_legacy_hash(&self, chain_id: ChainId) -> TransactionHash {
        LegacyHasher {
            prefix: felt_bytes!(b"deploy"),
            address: self.contract_address,
            entry_point: EntryPoint::CONSTRUCTOR,
            data_hash: self.constructor_calldata_hash(),
            nonce: None,
        }
        .hash(chain_id)
    }

    fn constructor_calldata_hash(&self) -> Felt {
        self.constructor_calldata
            .iter()
            .fold(PedersenHasher::default(), |hasher, data| {
                hasher.chain_update(data.0)
            })
            .finalize()
    }
}

impl DeployTransactionV1 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"deploy"),
            version: TransactionVersion::ONE.with_query_only(query_only),
            address: self.contract_address,
            entry_point: EntryPoint::CONSTRUCTOR,
            data_hash: self.constructor_calldata_hash(),
            ..Default::default()
        }
        .hash(chain_id)
    }

    fn calculate_legacy_hash(&self, chain_id: ChainId) -> TransactionHash {
        LegacyHasher {
            prefix: felt_bytes!(b"deploy"),
            address: self.contract_address,
            entry_point: EntryPoint::CONSTRUCTOR,
            data_hash: self.constructor_calldata_hash(),
            nonce: None,
        }
        .hash(chain_id)
    }

    fn constructor_calldata_hash(&self) -> Felt {
        self.constructor_calldata
            .iter()
            .fold(PedersenHasher::default(), |hasher, data| {
                hasher.chain_update(data.0)
            })
            .finalize()
    }
}

impl DeployAccountTransactionV1 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        let constructor_calldata_hash = std::iter::once(self.class_hash.0)
            .chain(std::iter::once(self.contract_address_salt.0))
            .chain(self.constructor_calldata.iter().map(|x| x.0))
            .fold(PedersenHasher::default(), |hasher, data| {
                hasher.chain_update(data)
            })
            .finalize();

        PreV3Hasher {
            prefix: felt_bytes!(b"deploy_account"),
            version: TransactionVersion::ONE.with_query_only(query_only),
            address: self.contract_address,
            data_hash: constructor_calldata_hash,
            max_fee: self.max_fee,
            nonce_or_class: Some(self.nonce.0),
            ..Default::default()
        }
        .hash(chain_id)
    }
}

impl InvokeTransactionV0 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"invoke"),
            version: TransactionVersion::ZERO.with_query_only(query_only),
            address: self.sender_address,
            entry_point: self.entry_point_selector,
            data_hash: self.calldata_hash(),
            max_fee: self.max_fee,
            ..Default::default()
        }
        .hash(chain_id)
    }

    fn calculate_legacy_hash(&self, chain_id: ChainId) -> TransactionHash {
        LegacyHasher {
            prefix: felt_bytes!(b"invoke"),
            address: self.sender_address,
            entry_point: self.entry_point_selector,
            data_hash: self.calldata_hash(),
            nonce: None,
        }
        .hash(chain_id)
    }

    fn calldata_hash(&self) -> Felt {
        self.calldata
            .iter()
            .fold(PedersenHasher::default(), |hasher, data| {
                hasher.chain_update(data.0)
            })
            .finalize()
    }
}

impl L1HandlerTransaction {
    pub fn calculate_message_hash(&self) -> H256 {
        use sha3::{Digest, Keccak256};

        let Some((from_address, payload)) = self.calldata.split_first() else {
            // This would indicate a pretty severe error in the L1 transaction.
            // But since we haven't encoded this during serialization, this could in
            // theory mess us up here.
            //
            // We should incorporate this into the deserialization instead. Returning an
            // error here is unergonomic and far too late.
            return H256::zero();
        };

        let mut hash = Keccak256::new();

        // This is an ethereum address
        hash.update(from_address.0.as_be_bytes());
        hash.update(self.contract_address.0.as_be_bytes());
        hash.update(self.nonce.0.as_be_bytes());
        hash.update(self.entry_point_selector.0.as_be_bytes());

        // Pad the u64 to 32 bytes to match a felt.
        hash.update([0u8; 24]);
        hash.update((payload.len() as u64).to_be_bytes());

        for elem in payload {
            hash.update(elem.0.as_be_bytes());
        }

        let hash = <[u8; 32]>::from(hash.finalize());

        hash.into()
    }

    pub fn calculate_hash(&self, chain_id: ChainId) -> TransactionHash {
        PreV3Hasher {
            prefix: felt_bytes!(b"l1_handler"),
            version: TransactionVersion::ZERO,
            address: self.contract_address,
            entry_point: self.entry_point_selector,
            data_hash: self.calldata_hash(),
            nonce_or_class: Some(self.nonce.0),
            ..Default::default()
        }
        .hash(chain_id)
    }

    fn calculate_legacy_hash(&self, chain_id: ChainId) -> TransactionHash {
        LegacyHasher {
            // Old L1 handler's were actually invokes under the hood.
            prefix: felt_bytes!(b"invoke"),
            address: self.contract_address,
            entry_point: self.entry_point_selector,
            data_hash: self.calldata_hash(),
            nonce: None,
        }
        .hash(chain_id)
    }

    // L1 handlers had a slightly different hash for Starknet v0.7.
    fn calculate_v07_hash(&self, chain_id: ChainId) -> TransactionHash {
        LegacyHasher {
            prefix: felt_bytes!(b"l1_handler"),
            address: self.contract_address,
            entry_point: self.entry_point_selector,
            data_hash: self.calldata_hash(),
            nonce: Some(self.nonce),
        }
        .hash(chain_id)
    }

    fn calldata_hash(&self) -> Felt {
        self.calldata
            .iter()
            .fold(PedersenHasher::default(), |hasher, data| {
                hasher.chain_update(data.0)
            })
            .finalize()
    }
}

impl DeclareTransactionV3 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        let deployment_hash = self
            .account_deployment_data
            .iter()
            .fold(PoseidonHasher::default(), |hasher, data| {
                hasher.chain(data.0.into())
            })
            .finish()
            .into();

        V3Hasher {
            prefix: felt_bytes!(b"declare"),
            sender_address: self.sender_address,
            nonce: self.nonce,
            data_hashes: &[
                deployment_hash,
                self.class_hash.0,
                self.compiled_class_hash.0,
            ],
            tip: self.tip,
            paymaster_data: &self.paymaster_data,
            nonce_data_availability_mode: self.nonce_data_availability_mode,
            fee_data_availability_mode: self.fee_data_availability_mode,
            resource_bounds: self.resource_bounds,
            query_only,
            proof_facts: &[],
        }
        .hash(chain_id)
    }
}

impl DeployAccountTransactionV3 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        let deployment_hash = self
            .constructor_calldata
            .iter()
            .fold(PoseidonHasher::default(), |hasher, data| {
                hasher.chain(data.0.into())
            })
            .finish()
            .into();

        V3Hasher {
            prefix: felt_bytes!(b"deploy_account"),
            sender_address: self.contract_address,
            nonce: self.nonce,
            data_hashes: &[
                deployment_hash,
                self.class_hash.0,
                self.contract_address_salt.0,
            ],
            tip: self.tip,
            paymaster_data: &self.paymaster_data,
            nonce_data_availability_mode: self.nonce_data_availability_mode,
            fee_data_availability_mode: self.fee_data_availability_mode,
            resource_bounds: self.resource_bounds,
            query_only,
            proof_facts: &[],
        }
        .hash(chain_id)
    }
}

impl InvokeTransactionV3 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        let deployment_hash = self
            .account_deployment_data
            .iter()
            .fold(PoseidonHasher::default(), |hasher, data| {
                hasher.chain(data.0.into())
            })
            .finish()
            .into();
        let calldata_hash = self
            .calldata
            .iter()
            .fold(PoseidonHasher::default(), |hasher, data| {
                hasher.chain(data.0.into())
            })
            .finish()
            .into();

        V3Hasher {
            prefix: felt_bytes!(b"invoke"),
            sender_address: self.sender_address,
            nonce: self.nonce,
            data_hashes: &[deployment_hash, calldata_hash],
            tip: self.tip,
            paymaster_data: &self.paymaster_data,
            nonce_data_availability_mode: self.nonce_data_availability_mode,
            fee_data_availability_mode: self.fee_data_availability_mode,
            resource_bounds: self.resource_bounds,
            query_only,
            proof_facts: &self.proof_facts,
        }
        .hash(chain_id)
    }
}

impl InvokeTransactionV1 {
    fn calculate_hash(&self, chain_id: ChainId, query_only: bool) -> TransactionHash {
        let list_hash = self
            .calldata
            .iter()
            .fold(PedersenHasher::default(), |hasher, data| {
                hasher.chain_update(data.0)
            })
            .finalize();

        PreV3Hasher {
            prefix: felt_bytes!(b"invoke"),
            version: TransactionVersion::ONE.with_query_only(query_only),
            address: self.sender_address,
            data_hash: list_hash,
            max_fee: self.max_fee,
            nonce_or_class: Some(self.nonce.0),
            ..Default::default()
        }
        .hash(chain_id)
    }
}

#[derive(Default)]
struct LegacyHasher {
    pub prefix: Felt,
    pub address: ContractAddress,
    pub entry_point: EntryPoint,
    pub data_hash: Felt,
    pub nonce: Option<TransactionNonce>,
}

impl LegacyHasher {
    fn hash(self, chain_id: ChainId) -> TransactionHash {
        let mut hasher = PedersenHasher::default()
            .chain_update(self.prefix)
            .chain_update(*self.address.get())
            .chain_update(self.entry_point.0)
            .chain_update(self.data_hash)
            .chain_update(chain_id.0);

        if let Some(nonce) = self.nonce {
            hasher.update(nonce.0);
        }

        TransactionHash(hasher.finalize())
    }
}

#[derive(Default)]
struct PreV3Hasher {
    pub prefix: Felt,
    pub version: TransactionVersion,
    pub address: ContractAddress,
    pub entry_point: EntryPoint,
    pub data_hash: Felt,
    pub max_fee: Fee,
    pub nonce_or_class: Option<Felt>,
    pub casm_hash: Option<CasmHash>,
}

impl PreV3Hasher {
    fn hash(self, chain_id: ChainId) -> TransactionHash {
        let mut hash = PedersenHasher::default()
            .chain_update(self.prefix)
            .chain_update(self.version.0)
            .chain_update(self.address.0)
            .chain_update(self.entry_point.0)
            .chain_update(self.data_hash)
            .chain_update(self.max_fee.0)
            .chain_update(chain_id.0);

        if let Some(felt) = self.nonce_or_class {
            hash.update(felt);
        }

        if let Some(felt) = self.casm_hash {
            hash.update(felt.0);
        }

        TransactionHash(hash.finalize())
    }
}

/// Provides hashing for V3 transactions.
struct V3Hasher<'a> {
    pub prefix: Felt,
    pub sender_address: ContractAddress,
    pub nonce: TransactionNonce,
    pub data_hashes: &'a [Felt],
    pub tip: Tip,
    pub paymaster_data: &'a [PaymasterDataElem],
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBounds,
    pub query_only: bool,
    pub proof_facts: &'a [ProofFactElem],
}

impl V3Hasher<'_> {
    fn hash(self, chain_id: ChainId) -> TransactionHash {
        let hasher = PoseidonHasher::default()
            .chain(self.prefix.into())
            .chain(
                TransactionVersion::THREE
                    .with_query_only(self.query_only)
                    .0
                    .into(),
            )
            .chain(self.sender_address.0.into())
            .chain(self.hash_fee_fields().into())
            .chain(self.hash_paymaster_data().into())
            .chain(chain_id.0.into())
            .chain(self.nonce.0.into())
            .chain(self.pack_data_availability().into());

        let hasher = self
            .data_hashes
            .iter()
            .fold(hasher, |hasher, &data| hasher.chain(data.into()));

        let hasher = if !self.proof_facts.is_empty() {
            let proof_facts_hash = self
                .proof_facts
                .iter()
                .fold(PoseidonHasher::default(), |hasher, data| {
                    hasher.chain(data.0.into())
                })
                .finish();

            hasher.chain(proof_facts_hash)
        } else {
            hasher
        };

        let hash = hasher.finish();

        TransactionHash(hash.into())
    }

    fn pack_data_availability(&self) -> u64 {
        let nonce = u64::from(self.nonce_data_availability_mode) << 32;
        let fee = u64::from(self.fee_data_availability_mode);

        nonce + fee
    }

    fn hash_paymaster_data(&self) -> Felt {
        self.paymaster_data
            .iter()
            .fold(PoseidonHasher::default(), |hasher, data| {
                hasher.chain(data.0.into())
            })
            .finish()
            .into()
    }

    fn hash_fee_fields(&self) -> Felt {
        let mut hasher = PoseidonHasher::default()
            .chain(self.tip.0.into())
            .chain(Self::pack_gas_bound(b"L1_GAS", &self.resource_bounds.l1_gas).into())
            .chain(Self::pack_gas_bound(b"L2_GAS", &self.resource_bounds.l2_gas).into());

        if let Some(l1_data_gas) = self.resource_bounds.l1_data_gas {
            hasher = hasher.chain(Self::pack_gas_bound(b"L1_DATA", &l1_data_gas).into());
        }

        hasher.finish().into()
    }

    fn pack_gas_bound(name: &[u8], bound: &ResourceBound) -> Felt {
        let mut buffer: [u8; 32] = Default::default();
        let (remainder, max_price) = buffer.split_at_mut(128 / 8);
        let (gas_kind, max_amount) = remainder.split_at_mut(64 / 8);

        let padding = gas_kind.len() - name.len();
        gas_kind[padding..].copy_from_slice(name);
        max_amount.copy_from_slice(&bound.max_amount.0.to_be_bytes());
        max_price.copy_from_slice(&bound.max_price_per_unit.0.to_be_bytes());

        Felt::from_be_bytes(buffer).expect("Packed resource should fit into felt")
    }
}

impl<T> Dummy<T> for DeclareTransactionV0V1 {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        Self {
            class_hash: Faker.fake_with_rng(rng),
            max_fee: Faker.fake_with_rng(rng),
            // This is to keep DeclareV0 p2p compliant
            nonce: TransactionNonce::ZERO,
            signature: Faker.fake_with_rng(rng),
            sender_address: Faker.fake_with_rng(rng),
        }
    }
}

impl<T> Dummy<T> for DeployTransactionV0 {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        let class_hash = Faker.fake_with_rng(rng);
        let contract_address_salt = Faker.fake_with_rng(rng);
        let constructor_calldata: Vec<ConstructorParam> = Faker.fake_with_rng(rng);
        let contract_address = ContractAddress::deployed_contract_address(
            constructor_calldata.iter().map(|d| CallParam(d.0)),
            &contract_address_salt,
            &class_hash,
        );

        Self {
            class_hash,
            contract_address,
            contract_address_salt,
            constructor_calldata,
        }
    }
}

impl<T> Dummy<T> for DeployTransactionV1 {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        let class_hash = Faker.fake_with_rng(rng);
        let contract_address_salt = Faker.fake_with_rng(rng);
        let constructor_calldata: Vec<ConstructorParam> = Faker.fake_with_rng(rng);
        let contract_address = ContractAddress::deployed_contract_address(
            constructor_calldata.iter().map(|d| CallParam(d.0)),
            &contract_address_salt,
            &class_hash,
        );

        Self {
            class_hash,
            contract_address,
            contract_address_salt,
            constructor_calldata,
        }
    }
}

impl<T> Dummy<T> for DeployAccountTransactionV1 {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        let class_hash = Faker.fake_with_rng(rng);
        let contract_address_salt = Faker.fake_with_rng(rng);
        let constructor_calldata: Vec<CallParam> = Faker.fake_with_rng(rng);
        let contract_address = ContractAddress::deployed_contract_address(
            constructor_calldata.iter().map(|d| CallParam(d.0)),
            &contract_address_salt,
            &class_hash,
        );

        Self {
            contract_address,
            max_fee: Faker.fake_with_rng(rng),
            signature: Faker.fake_with_rng(rng),
            nonce: Faker.fake_with_rng(rng),
            contract_address_salt,
            constructor_calldata,
            class_hash,
        }
    }
}

impl<T> Dummy<T> for DeployAccountTransactionV3 {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        let class_hash = Faker.fake_with_rng(rng);
        let contract_address_salt = Faker.fake_with_rng(rng);
        let constructor_calldata: Vec<CallParam> = Faker.fake_with_rng(rng);
        let contract_address = ContractAddress::deployed_contract_address(
            constructor_calldata.iter().map(|d| CallParam(d.0)),
            &contract_address_salt,
            &class_hash,
        );
        Self {
            contract_address,
            signature: Faker.fake_with_rng(rng),
            nonce: Faker.fake_with_rng(rng),
            nonce_data_availability_mode: Faker.fake_with_rng(rng),
            fee_data_availability_mode: Faker.fake_with_rng(rng),
            resource_bounds: Faker.fake_with_rng(rng),
            tip: Faker.fake_with_rng(rng),
            paymaster_data: Faker.fake_with_rng(rng),
            contract_address_salt,
            constructor_calldata,
            class_hash,
        }
    }
}

impl<T> Dummy<T> for InvokeTransactionV0 {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        Self {
            calldata: Faker.fake_with_rng(rng),
            sender_address: Faker.fake_with_rng(rng),
            entry_point_selector: Faker.fake_with_rng(rng),
            // This is a legacy field, not used in p2p
            entry_point_type: None,
            max_fee: Faker.fake_with_rng(rng),
            signature: Faker.fake_with_rng(rng),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macro_prelude::*;

    // Goerli support was removed, however some of the fixtures originally come from
    // Goerli.
    const GOERLI_TESTNET: ChainId = ChainId(match Felt::from_be_slice(b"SN_GOERLI") {
        Ok(chain_id) => chain_id,
        Err(_) => unreachable!(),
    });

    #[rstest::rstest]
    #[case::declare_v0(declare_v0(), GOERLI_TESTNET)]
    #[case::declare_v1(declare_v1(), ChainId::SEPOLIA_TESTNET)]
    #[case::declare_v2(declare_v2(), ChainId::SEPOLIA_TESTNET)]
    #[case::declare_v3(declare_v3(), GOERLI_TESTNET)]
    #[case::deploy(deploy(), GOERLI_TESTNET)]
    #[case::deploy_legacy(deploy_legacy(), GOERLI_TESTNET)]
    #[case::deploy_account_v1(deploy_account_v1(), ChainId::MAINNET)]
    #[case::deploy_account_v3(deploy_account_v3(), GOERLI_TESTNET)]
    #[case::invoke_v0(invoke_v0(), GOERLI_TESTNET)]
    #[case::invoke_v0_legacy(invoke_v0_legacy(), GOERLI_TESTNET)]
    #[case::invoke_v1(invoke_v1(), ChainId::MAINNET)]
    #[case::invoke_v3(invoke_v3(), ChainId::SEPOLIA_TESTNET)]
    #[case::invoke_v3_with_proof_facts(invoke_v3_with_proof_facts(), ChainId::MAINNET)]
    #[case::l1_handler(l1_handler(), ChainId::MAINNET)]
    #[case::l1_handler_v07(l1_handler_v07(), ChainId::MAINNET)]
    #[case::l1_handler_legacy(l1_handler_legacy(), GOERLI_TESTNET)]
    fn verify_hash(#[case] transaction: Transaction, #[case] chain_id: ChainId) {
        assert!(transaction.verify_hash(chain_id));
    }

    fn declare_v0() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x6d346ba207eb124355960c19c737698ad37a3c920a588b741e0130ff5bd4d6d"
            ),
            variant: TransactionVariant::DeclareV0(DeclareTransactionV0V1 {
                class_hash: class_hash!(
                    "0x71e6ef53e53e6f5ca792fc4a5799a33e6f4118e4fd1d948dca3a371506f0cc7"
                ),
                sender_address: contract_address!("0x1"),
                ..Default::default()
            }),
        }
    }

    fn declare_v1() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0xb2d88f64d9655a7d47a5519d66b969168d02d0d33f6476f0d2539c51686329"
            ),
            variant: TransactionVariant::DeclareV1(DeclareTransactionV0V1 {
                class_hash: class_hash!(
                    "0x3131fa018d520a037686ce3efddeab8f28895662f019ca3ca18a626650f7d1e"
                ),
                max_fee: fee!("0x625e5879c08f4"),
                nonce: transaction_nonce!("0x7"),
                signature: vec![
                    transaction_signature_elem!(
                        "0x3609667964a8ed946bc507721ec35a851d97a097d159ef0ec2af8fab490223f"
                    ),
                    transaction_signature_elem!(
                        "0x68846bad9f0f010fac4eeaf39f9dd609b28765fd2336b70ce026e33e2421c15"
                    ),
                ],
                sender_address: contract_address!(
                    "0x68922eb87daed71fc3099031e178b6534fc39a570022342e8c166024da893f5"
                ),
            }),
        }
    }

    fn declare_v2() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x4cacc2bbdd5ec77b20e908f311ab27d6495b69761e929bb24ba02632716944"
            ),
            variant: TransactionVariant::DeclareV2(DeclareTransactionV2 {
                class_hash: class_hash!(
                    "0x1a736d6ed154502257f02b1ccdf4d9d1089f80811cd6acad48e6b6a9d1f2003"
                ),
                max_fee: fee!("0x92fa1ac712614"),
                nonce: transaction_nonce!("0x6"),
                signature: vec![
                    transaction_signature_elem!(
                        "0x4ab3e77908396c66b39326f52334b447fe878d1d899a287c9e3cf7bd09839ea"
                    ),
                    transaction_signature_elem!(
                        "0x79a56f9e61eb834f1ac524eb35da33cccf92ff3b01a7a8eaf68cbb64bebdba9"
                    ),
                ],
                sender_address: contract_address!(
                    "0x68922eb87daed71fc3099031e178b6534fc39a570022342e8c166024da893f5"
                ),
                compiled_class_hash: casm_hash!(
                    "0x29787a427a423ffc5986d43e630077a176e4391fcef3ebf36014b154069ae4"
                ),
            }),
        }
    }

    fn declare_v3() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x41d1f5206ef58a443e7d3d1ca073171ec25fa75313394318fc83a074a6631c3"
            ),
            variant: TransactionVariant::DeclareV3(DeclareTransactionV3 {
                signature: vec![
                    transaction_signature_elem!(
                        "0x29a49dff154fede73dd7b5ca5a0beadf40b4b069f3a850cd8428e54dc809ccc"
                    ),
                    transaction_signature_elem!(
                        "0x429d142a17223b4f2acde0f5ecb9ad453e188b245003c86fab5c109bad58fc3"
                    ),
                ],
                nonce: transaction_nonce!("0x1"),
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(0x186a0),
                        max_price_per_unit: ResourcePricePerUnit(0x2540be400),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                sender_address: contract_address!(
                    "0x2fab82e4aef1d8664874e1f194951856d48463c3e6bf9a8c68e234a629a6f50"
                ),
                class_hash: class_hash!(
                    "0x5ae9d09292a50ed48c5930904c880dab56e85b825022a7d689cfc9e65e01ee7"
                ),
                compiled_class_hash: casm_hash!(
                    "0x1add56d64bebf8140f3b8a38bdf102b7874437f0c861ab4ca7526ec33b4d0f8"
                ),
                ..Default::default()
            }),
        }
    }

    fn deploy() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x3d7623443283d9a0cec946492db78b06d57642a551745ddfac8d3f1f4fcc2a8"
            ),
            variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                contract_address: contract_address!(
                    "0x54c6883e459baeac4a9052ee109b86b9f81adbcdcb1f65a05dceec4c34d5cf9"
                ),
                contract_address_salt: contract_address_salt!(
                    "0x655a594122f68f5e821834e606e1243b249a88555fac2d548f7acbee7863f62"
                ),
                constructor_calldata: vec![
                    constructor_param!(
                        "0x734d2849eb47e10c59e5a433d425675849cb37338b1d7c4c4afb1e0ca42133"
                    ),
                    constructor_param!(
                        "0xffad0128dbd859ef97a246a2d2c00680dedc8d850ff9b6ebcc8b94ee9625bb"
                    ),
                ],
                class_hash: class_hash!(
                    "0x3523d31a077d891b4d888f9d3c7d33bdac2c0a06f89c08307a7f7b68f681c98"
                ),
            }),
        }
    }

    fn deploy_legacy() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x45c61314be4da85f0e13df53d18062e002c04803218f08061e4b274d4b38537"
            ),
            variant: TransactionVariant::DeployV0(DeployTransactionV0 {
                contract_address: contract_address!(
                    "0x2f40faa63fdd5871415b2dcfb1a5e3e1ca06435b3dda6e2ba9df3f726fd3251"
                ),
                contract_address_salt: contract_address_salt!(
                    "0x7284a0367fdd636434f76da25532785690d5f27db40ba38b0cfcbc89a472507"
                ),
                constructor_calldata: vec![
                    constructor_param!(
                        "0x635b73abaa9efff71570cb08f3e5014424788470c3b972b952368fb3fc27cc3"
                    ),
                    constructor_param!(
                        "0x7e92479a573a24241ee6f3e4ade742ff37bae4a60bacef5be1caaff5e7e04f3"
                    ),
                ],
                class_hash: class_hash!(
                    "0x10455c752b86932ce552f2b0fe81a880746649b9aee7e0d842bf3f52378f9f8"
                ),
            }),
        }
    }

    fn deploy_account_v1() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x63b72dba5a1b5cdd2585b0c7103242244860453f7013023c1a21f32e1863ec"
            ),
            variant: TransactionVariant::DeployAccountV1(DeployAccountTransactionV1 {
                contract_address: contract_address!(
                    "0x3faed8332496d9de9c546e7942b35ba3ea323a6af72d6033f746ea60ecc02ef"
                ),
                max_fee: fee!("0xb48040809d4b"),
                signature: vec![
                    transaction_signature_elem!(
                        "0x463d21c552a810c59be86c336c0cc68f28e3815eafbe1a2eaf9b3a6fe1c2b82"
                    ),
                    transaction_signature_elem!(
                        "0x2932cb2583da5d8d08f6f0179cc3d4aaae2b46123f02f00bfd544105671adfd"
                    ),
                ],
                nonce: transaction_nonce!("0x0"),
                contract_address_salt: contract_address_salt!(
                    "0x771b3077f205e2d77c06c9a3bd49d730a4fd8453941d031009fa40936912030"
                ),
                constructor_calldata: vec![
                    call_param!(
                        "0x771b3077f205e2d77c06c9a3bd49d730a4fd8453941d031009fa40936912030"
                    ),
                    call_param!("0x0"),
                ],
                class_hash: class_hash!(
                    "0x1a736d6ed154502257f02b1ccdf4d9d1089f80811cd6acad48e6b6a9d1f2003"
                ),
            }),
        }
    }

    fn deploy_account_v3() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x29fd7881f14380842414cdfdd8d6c0b1f2174f8916edcfeb1ede1eb26ac3ef0"
            ),
            variant: TransactionVariant::DeployAccountV3(DeployAccountTransactionV3 {
                contract_address: contract_address!(
                    "0x2fab82e4aef1d8664874e1f194951856d48463c3e6bf9a8c68e234a629a6f50"
                ),
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(0x186a0),
                        max_price_per_unit: ResourcePricePerUnit(0x5af3107a4000),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                constructor_calldata: vec![call_param!(
                    "0x5cd65f3d7daea6c63939d659b8473ea0c5cd81576035a4d34e52fb06840196c"
                )],
                class_hash: class_hash!(
                    "0x2338634f11772ea342365abd5be9d9dc8a6f44f159ad782fdebd3db5d969738"
                ),
                signature: vec![
                    transaction_signature_elem!(
                        "0x6d756e754793d828c6c1a89c13f7ec70dbd8837dfeea5028a673b80e0d6b4ec"
                    ),
                    transaction_signature_elem!(
                        "0x4daebba599f860daee8f6e100601d98873052e1c61530c630cc4375c6bd48e3"
                    ),
                ],
                ..Default::default()
            }),
        }
    }

    fn invoke_v0() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x587d93f2339b7f2beda040187dbfcb9e076ce4a21eb8d15ae64819718817fbe"
            ),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract_address!(
                    "0x7463cdd01f6e6a4f13084ea9eee170298b0bbe3faa17f46924c85bb284d4c98"
                ),
                max_fee: fee!("0x1ee7b2b881350"),
                signature: vec![
                    transaction_signature_elem!(
                        "0x6e82c6752bd13e29b68cf0c8b0d4eb9133b5a056336a842bff01756e514d04a"
                    ),
                    transaction_signature_elem!(
                        "0xa87f00c9e39fd0711aaea4edae0f00044384188a87f489170ac383e3ad087f"
                    ),
                ],
                calldata: vec![
                    call_param!("0x3"),
                    call_param!(
                        "0x72df4dc5b6c4df72e4288857317caf2ce9da166ab8719ab8306516a2fddfff7"
                    ),
                    call_param!(
                        "0x219209e083275171774dab1df80982e9df2096516f06319c5c6d71ae0a8480c"
                    ),
                    call_param!("0x0"),
                    call_param!("0x3"),
                    call_param!(
                        "0x7394cbe418daa16e42b87ba67372d4ab4a5df0b05c6e554d158458ce245bc10"
                    ),
                    call_param!(
                        "0x219209e083275171774dab1df80982e9df2096516f06319c5c6d71ae0a8480c"
                    ),
                    call_param!("0x3"),
                    call_param!("0x3"),
                    call_param!(
                        "0x4aec73f0611a9be0524e7ef21ab1679bdf9c97dc7d72614f15373d431226b6a"
                    ),
                    call_param!(
                        "0x3f35dbce7a07ce455b128890d383c554afbc1b07cf7390a13e2d602a38c1a0a"
                    ),
                    call_param!("0x6"),
                    call_param!("0xa"),
                    call_param!("0x10"),
                    call_param!(
                        "0x4aec73f0611a9be0524e7ef21ab1679bdf9c97dc7d72614f15373d431226b6a"
                    ),
                    call_param!("0x14934a76f"),
                    call_param!("0x0"),
                    call_param!(
                        "0x4aec73f0611a9be0524e7ef21ab1679bdf9c97dc7d72614f15373d431226b6a"
                    ),
                    call_param!("0x2613cd2f52b54fb440"),
                    call_param!("0x0"),
                    call_param!(
                        "0x72df4dc5b6c4df72e4288857317caf2ce9da166ab8719ab8306516a2fddfff7"
                    ),
                    call_param!(
                        "0x7394cbe418daa16e42b87ba67372d4ab4a5df0b05c6e554d158458ce245bc10"
                    ),
                    call_param!("0x14934a76f"),
                    call_param!("0x0"),
                    call_param!("0x2613cd2f52b54fb440"),
                    call_param!("0x0"),
                    call_param!("0x135740b18"),
                    call_param!("0x0"),
                    call_param!("0x23caeef429e7df66e0"),
                    call_param!("0x0"),
                    call_param!("0x17"),
                ],
                entry_point_selector: entry_point!(
                    "0x15d40a3d6ca2ac30f4031e42be28da9b056fef9bb7357ac5e85627ee876e5ad"
                ),
                ..Default::default()
            }),
        }
    }

    fn invoke_v0_legacy() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x493d8fab73af67e972788e603aee18130facd3c7685f16084ecd98b07153e24"
            ),
            variant: TransactionVariant::InvokeV0(InvokeTransactionV0 {
                sender_address: contract_address!(
                    "0x639322e9822149638b70f6de65bc18f3563bd6fa16f0106e8162618eb72f7e"
                ),
                calldata: vec![
                    call_param!(
                        "0x49e2e40a0b61a4d6fe4c85cbbf61b5ba372427c852f88509350c4b1eeb88426"
                    ),
                    call_param!("0x2"),
                    call_param!(
                        "0x1576521d9ed09609f55b86740de4ae6abdb2837d5d960ae71083ccd39c715d2"
                    ),
                    call_param!(
                        "0x6897cf3003dc45dd016a34ee4309fc97f3bd471513553e64bc070b4eedf4eae"
                    ),
                ],
                entry_point_selector: entry_point!(
                    "0x317eb442b72a9fae758d4fb26830ed0d9f31c8e7da4dbff4e8c59ea6a158e7f"
                ),
                ..Default::default()
            }),
        }
    }

    fn invoke_v1() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x53ee528f0572d6e43b3318ba59a02be15d51f66d8b5dc1f84af2ccbe606e769"
            ),
            variant: TransactionVariant::InvokeV1(InvokeTransactionV1 {
                sender_address: contract_address!(
                    "0x3b184c08ea47b80bbe024f42ca94210de552fe2096b0907b6a45809fee82779"
                ),
                max_fee: fee!("0x125c44c433000"),
                nonce: transaction_nonce!("0x1b"),
                signature: vec![
                    transaction_signature_elem!(
                        "0x50e7acc40dcdcad7bf5a758a85f6676620be6f76668913e07c58c4a8d4a45f8"
                    ),
                    transaction_signature_elem!(
                        "0x5eb8f2407a69ed0c19565267c0c67b588056f7201e471d687a3041be3732f35"
                    ),
                ],
                calldata: vec![
                    call_param!("0x1"),
                    call_param!(
                        "0x4c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"
                    ),
                    call_param!(
                        "0x2f68935fe2620d447e6dee46fb77624aee380c157f7675e9e4220599f4a04bd"
                    ),
                    call_param!("0x0"),
                    call_param!("0x1"),
                    call_param!("0x1"),
                    call_param!(
                        "0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8"
                    ),
                ],
            }),
        }
    }

    fn invoke_v3() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x22772429229cbca26cb062f6f6a0991a4e84d0f11f3b1bda1913613a5e609e0"
            ),
            variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                signature: vec![
                    transaction_signature_elem!(
                        "0x389bca189562763f6a73da4aaab30d87d8bbc243571f4a353c48493a43a0634"
                    ),
                    transaction_signature_elem!(
                        "0x62d30041a0b1199b3ad93515066d5c7791211fa32f585956fafe630082270e9"
                    ),
                ],
                nonce: transaction_nonce!("0x1084b"),
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(0x61a80),
                        max_price_per_unit: ResourcePricePerUnit(0x5af3107a4000),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                sender_address: contract_address!(
                    "0x35acd6dd6c5045d18ca6d0192af46b335a5402c02d41f46e4e77ea2c951d9a3"
                ),
                calldata: vec![
                    call_param!("0x1"),
                    call_param!(
                        "0x47ad6a25df680763e5663bd0eba3d2bfd18b24b1e8f6bd36b71c37433c63ed0"
                    ),
                    call_param!(
                        "0x19a35a6e95cb7a3318dbb244f20975a1cd8587cc6b5259f15f61d7beb7ee43b"
                    ),
                    call_param!("0x2"),
                    call_param!(
                        "0x4d0b88ace5705bb7825f91ee95557d906600b7e7762f5615e6a4f407185a43a"
                    ),
                    call_param!(
                        "0x630ac7edd6c7c097e4f9774fe5855bed3a2b8886286c61f1f7afd601e124d60"
                    ),
                ],
                ..Default::default()
            }),
        }
    }

    // A variant of invoke_v3 with proof facts.
    // Taken from `starknet_api` tests: https://github.com/starkware-libs/sequencer/blob/2a5c56209c2bd8d2be5379c58eba32cf2ba0391d/crates/starknet_api/resources/transaction_hash.json#L116
    fn invoke_v3_with_proof_facts() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x6d885b1a2b7cb7946480c63aa1697888a33e9ccd0b1516f41c41731a1628726"
            ),
            variant: TransactionVariant::InvokeV3(InvokeTransactionV3 {
                signature: vec![
                    transaction_signature_elem!(
                        "0x389bca189562763f6a73da4aaab30d87d8bbc243571f4a353c48493a43a0634"
                    ),
                    transaction_signature_elem!(
                        "0x62d30041a0b1199b3ad93515066d5c7791211fa32f585956fafe630082270e9"
                    ),
                ],
                nonce: transaction_nonce!("0x9d"),
                nonce_data_availability_mode: DataAvailabilityMode::L1,
                fee_data_availability_mode: DataAvailabilityMode::L1,
                resource_bounds: ResourceBounds {
                    l1_gas: ResourceBound {
                        max_amount: ResourceAmount(0xa9e),
                        max_price_per_unit: ResourcePricePerUnit(0x7f2a1ad4f2f1),
                    },
                    l2_gas: Default::default(),
                    l1_data_gas: Default::default(),
                },
                sender_address: contract_address!(
                    "0x69c0f9bcd79697bdceaf7748e3ff8f34aa39e4063ce44896af664c0c96f6c10"
                ),
                calldata: vec![
                    call_param!("0x1"),
                    call_param!(
                        "0x4c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"
                    ),
                    call_param!(
                        "0x3943907ef0ef6f9d2e2408b05e520a66daaf74293dbf665e5a20b117676170e"
                    ),
                    call_param!("0x2"),
                    call_param!(
                        "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"
                    ),
                    call_param!("0x16345785d8a0000"),
                ],
                proof_facts: vec![
                    proof_fact_elem!("0x1"),
                    proof_fact_elem!("0x2"),
                    proof_fact_elem!("0x3"),
                ],
                ..Default::default()
            }),
        }
    }

    fn l1_handler() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x8d7d99f96167a01f2406ae25dd6bdeb4f903fd4ed433d96dcf2564b7ab0a8f"
            ),
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!(
                    "0x73314940630fd6dcda0d772d4c972c4e0a9946bef9dabf4ef84eda8ef542b82"
                ),
                entry_point_selector: entry_point!(
                    "0x2d757788a8d8d6f21d1cd40bce38a8222d70654214e96ff95d8086e684fbee5"
                ),
                nonce: transaction_nonce!("0x18cb20"),
                calldata: vec![
                    call_param!("0xae0ee0a63a2ce6baeeffe56e7714fb4efe48d419"),
                    call_param!(
                        "0x13f55ae8d173a036cf8bdf0448f04b835a5d42cda5fe6b4678217ed92cabc94"
                    ),
                    call_param!("0xd7621dc58210000"),
                    call_param!("0x0"),
                ],
            }),
        }
    }

    fn l1_handler_v07() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0x61b518bb1f97c49244b8a7a1a984798b4c2876d42920eca2b6ba8dfb1bddc54"
            ),
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!(
                    "0xda8054260ec00606197a4103eb2ef08d6c8af0b6a808b610152d1ce498f8c3"
                ),
                entry_point_selector: entry_point!(
                    "0xe3f5e9e1456ffa52a3fbc7e8c296631d4cc2120c0be1e2829301c0d8fa026b"
                ),
                nonce: transaction_nonce!("0x0"),
                calldata: vec![
                    call_param!("0x142273bcbfca76512b2a05aed21f134c4495208"),
                    call_param!("0xa0c316cb0bb0c9632315ddc8f49c7921f2c80daa"),
                    call_param!("0x2"),
                    call_param!(
                        "0x453b0310bcdfa50d3c2e7f757e284ac6cd4171933a4e67d1bdcfdbc7f3cbc93"
                    ),
                ],
            }),
        }
    }

    fn l1_handler_legacy() -> Transaction {
        Transaction {
            hash: transaction_hash!(
                "0xfb118dc1d4a4141b7718da4b7fa98980b11caf5aa5d6e1e35e9b050aae788b"
            ),
            variant: TransactionVariant::L1Handler(L1HandlerTransaction {
                contract_address: contract_address!(
                    "0x55a46448decca3b138edf0104b7a47d41365b8293bdfd59b03b806c102b12b7"
                ),
                entry_point_selector: entry_point!(
                    "0xc73f681176fc7b3f9693986fd7b14581e8d540519e27400e88b8713932be01"
                ),
                calldata: vec![
                    call_param!("0x2db8c2615db39a5ed8750b87ac8f217485be11ec"),
                    call_param!("0xbc614e"),
                    call_param!("0x258"),
                ],
                ..Default::default()
            }),
        }
    }

    #[test]
    fn invoke_v1_with_query_version() {
        let invoke = TransactionVariant::InvokeV1(InvokeTransactionV1 {
            sender_address: contract_address!(
                "0x05b53783880534bf39ba4224ffbf6cdbca5d9f8f018a21cf1fe870dff409b3ce"
            ),
            max_fee: fee!("0x0"),
            nonce: transaction_nonce!("0x3"),
            signature: vec![
                transaction_signature_elem!(
                    "0x29784653f04451ad9abb301d06320816756396b0bda4e598559eff4718fe6f9"
                ),
                transaction_signature_elem!(
                    "0x6c5288a10f44612ffdbfa8681af54f97e53339a5119713bdee36a05485abe60"
                ),
            ],
            calldata: vec![
                call_param!("0x1"),
                call_param!("0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"),
                call_param!("0x83afd3f4caedc6eebf44246fe54e38c95e3179a5ec9ea81740eca5b482d12e"),
                call_param!("0x0"),
                call_param!("0x3"),
                call_param!("0x3"),
                call_param!("0x5b53783880534bf39ba4224ffbf6cdbca5d9f8f018a21cf1fe870dff409b3ce"),
                call_param!("0x9184e72a000"),
                call_param!("0x0"),
            ],
        });

        assert_eq!(
            transaction_hash!("0x00acd1213b669b094390c5b70a447cb2335ee40bbe21c4544db57450aa0e5c04"),
            invoke.calculate_hash(GOERLI_TESTNET, true)
        );
    }
}
