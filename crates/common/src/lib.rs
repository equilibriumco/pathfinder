//! Contains core functions and types that are widely used but have no real
//! home of their own.
//!
//! This includes many trivial wrappers around [Felt] which help by providing
//! additional type safety.
use std::fmt::Display;
use std::ops::Rem;
use std::str::FromStr;

use anyhow::Context;
use fake::Dummy;
use pathfinder_crypto::hash::HashChain;
use pathfinder_crypto::Felt;
use primitive_types::H160;
use serde::{Deserialize, Serialize};

pub mod casm_class;
pub mod class_definition;
pub mod consensus_info;
pub mod consts;
pub mod event;
pub mod hash;
mod header;
pub mod integration_testing;
mod l1;
mod l2;
mod macros;
pub mod prelude;
pub mod receipt;
pub mod signature;
pub mod state_update;
pub mod test_utils;
pub mod transaction;
pub mod trie;

pub use header::{BlockHeader, BlockHeaderBuilder, L1DataAvailabilityMode, SignedBlockHeader};
pub use l1::{L1BlockHash, L1BlockNumber, L1TransactionHash};
pub use l2::{
    ConsensusFinalizedBlockHeader,
    ConsensusFinalizedL2Block,
    DecidedBlock,
    DecidedBlocks,
    DeclaredClass,
    L2Block,
    L2BlockToCommit,
};
pub use signature::BlockCommitmentSignature;
pub use state_update::{FoundStorageValue, StateUpdate};

impl ContractAddress {
    /// The contract at 0x1 is special. It was never deployed and therefore
    /// has no class hash. It does however receive storage changes.
    ///
    /// It is used by starknet to store values for smart contracts to access
    /// using syscalls. For example the block hash.
    pub const ONE: ContractAddress = contract_address!("0x1");
    /// The contract at 0x2 was introduced in Starknet version 0.13.4. It is
    /// used for stateful compression:
    /// - storage key 0 points to the global counter, which is the base for
    ///   index values in the next block,
    /// - other storage k-v pairs store the mapping of key to index,
    /// - the global counter starts at value 0x80 in the first block from
    ///   0.13.4,
    /// - keys of value lower than 0x80 are not indexed.
    pub const TWO: ContractAddress = contract_address!("0x2");
    /// Useful for iteration over the system contracts
    pub const SYSTEM: [ContractAddress; 2] = [ContractAddress::ONE, ContractAddress::TWO];
}

// Bytecode and entry point list of a class
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractClass {
    // A base64 encoding of the gzip-compressed JSON representation of program.
    pub program: String,
    // A JSON representation of the entry points
    // We don't actually process this value, just serialize/deserialize
    // from an already validated JSON.
    // This is kept as a Value to avoid dependency on sequencer API types.
    pub entry_points_by_type: serde_json::Value,
}

impl EntryPoint {
    /// Returns a new EntryPoint which has been truncated to fit from Keccak256
    /// digest of input.
    ///
    /// See: <https://docs.starknet.io/documentation/architecture_and_concepts/Smart_Contracts/contract-classes/>
    pub fn hashed(input: &[u8]) -> Self {
        use sha3::Digest;
        EntryPoint(truncated_keccak(<[u8; 32]>::from(sha3::Keccak256::digest(
            input,
        ))))
    }

    /// The constructor [EntryPoint], defined as the truncated keccak of
    /// b"constructor".
    pub const CONSTRUCTOR: Self =
        entry_point!("0x028FFE4FF0F226A9107253E17A904099AA4F63A02A5621DE0576E5AA71BC5194");
}

impl StateCommitment {
    /// Calculates global state commitment by combining the storage and class
    /// commitment.
    ///
    /// See
    /// <https://github.com/starkware-libs/cairo-lang/blob/12ca9e91bbdc8a423c63280949c7e34382792067/src/starkware/starknet/core/os/state.cairo#L125>
    /// for details.
    ///
    /// Starting from Starknet 0.14.0, the state commitment always uses the
    /// Poseidon hash formula, even when `class_commitment` is zero. For older
    /// versions, when `class_commitment` is zero, the state commitment equals
    /// the storage commitment directly.
    pub fn calculate(
        storage_commitment: StorageCommitment,
        class_commitment: ClassCommitment,
        version: StarknetVersion,
    ) -> Self {
        if class_commitment == ClassCommitment::ZERO
            && storage_commitment == StorageCommitment::ZERO
        {
            return StateCommitment::ZERO;
        }

        if class_commitment == ClassCommitment::ZERO && version < StarknetVersion::V_0_14_0 {
            return Self(storage_commitment.0);
        }

        const GLOBAL_STATE_VERSION: Felt = felt_bytes!(b"STARKNET_STATE_V0");

        StateCommitment(
            pathfinder_crypto::hash::poseidon::poseidon_hash_many(&[
                GLOBAL_STATE_VERSION.into(),
                storage_commitment.0.into(),
                class_commitment.0.into(),
            ])
            .into(),
        )
    }
}

impl StorageAddress {
    pub fn from_name(input: &[u8]) -> Self {
        use sha3::Digest;
        Self(truncated_keccak(<[u8; 32]>::from(sha3::Keccak256::digest(
            input,
        ))))
    }

    pub fn from_map_name_and_key(name: &[u8], key: Felt) -> Self {
        use sha3::Digest;

        let intermediate = truncated_keccak(<[u8; 32]>::from(sha3::Keccak256::digest(name)));
        let value = pathfinder_crypto::hash::pedersen_hash(intermediate, key);

        let value = primitive_types::U256::from_big_endian(value.as_be_bytes());
        let max_address = primitive_types::U256::from_str_radix(
            "0x7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00",
            16,
        )
        .unwrap();

        let value = value.rem(max_address);
        let b = value.to_big_endian();
        Self(Felt::from_be_slice(&b).expect("Truncated value should fit into a felt"))
    }
}

/// A Starknet block number.
#[derive(Copy, Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockNumber(u64);

macros::i64_backed_u64::new_get_partialeq!(BlockNumber);
macros::i64_backed_u64::serdes!(BlockNumber);

impl From<BlockNumber> for Felt {
    fn from(x: BlockNumber) -> Self {
        Felt::from(x.0)
    }
}

impl std::iter::Iterator for BlockNumber {
    type Item = BlockNumber;

    fn next(&mut self) -> Option<Self::Item> {
        Some(*self + 1)
    }
}

/// The timestamp of a Starknet block.
#[derive(Copy, Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockTimestamp(u64);

macros::i64_backed_u64::new_get_partialeq!(BlockTimestamp);
macros::i64_backed_u64::serdes!(BlockTimestamp);

/// A Starknet transaction index.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct TransactionIndex(u64);

macros::i64_backed_u64::new_get_partialeq!(TransactionIndex);
macros::i64_backed_u64::serdes!(TransactionIndex);

/// Starknet gas price.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Dummy)]
pub struct GasPrice(pub u128);

/// A hex representation of a [GasPrice].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Dummy)]
pub struct GasPriceHex(pub GasPrice);

/// Starknet resource bound: amount.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Dummy)]
pub struct ResourceAmount(pub u64);

// Transaction tip: the prioritization metric determines the sorting order of
// transactions in the mempool.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Dummy)]
pub struct Tip(pub u64);

// A hex representation of a [Tip].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Dummy)]
pub struct TipHex(pub Tip);

/// Starknet resource bound: price per unit.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Dummy)]
pub struct ResourcePricePerUnit(pub u128);

/// Starknet transaction version.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default, Dummy)]
pub struct TransactionVersion(pub Felt);

impl TransactionVersion {
    /// Checks if version is zero, handling QUERY_VERSION_BASE.
    pub fn is_zero(&self) -> bool {
        self.without_query_version() == 0
    }

    /// Returns the transaction version without QUERY_VERSION_BASE.
    ///
    /// QUERY_VERSION_BASE (2**128) is a large constant that gets
    /// added to the real version to make sure transactions constructed for
    /// call or estimateFee cannot be submitted for inclusion on the chain.
    pub fn without_query_version(&self) -> u128 {
        let lower = &self.0.as_be_bytes()[16..];
        u128::from_be_bytes(lower.try_into().expect("slice should be the right length"))
    }

    pub const fn with_query_version(self) -> Self {
        let mut bytes = self.0.to_be_bytes();
        bytes[15] |= 0b0000_0001;

        let felt = match Felt::from_be_bytes(bytes) {
            Ok(x) => x,
            Err(_) => panic!("Adding query bit to transaction version failed."),
        };
        Self(felt)
    }

    pub const fn has_query_version(&self) -> bool {
        self.0.as_be_bytes()[15] & 0b0000_0001 != 0
    }

    pub fn with_query_only(self, query_only: bool) -> Self {
        if query_only {
            self.with_query_version()
        } else {
            Self(self.without_query_version().into())
        }
    }

    pub const ZERO: Self = Self(Felt::ZERO);
    pub const ONE: Self = Self(Felt::from_u64(1));
    pub const TWO: Self = Self(Felt::from_u64(2));
    pub const THREE: Self = Self(Felt::from_u64(3));
    pub const ZERO_WITH_QUERY_VERSION: Self = Self::ZERO.with_query_version();
    pub const ONE_WITH_QUERY_VERSION: Self = Self::ONE.with_query_version();
    pub const TWO_WITH_QUERY_VERSION: Self = Self::TWO.with_query_version();
    pub const THREE_WITH_QUERY_VERSION: Self = Self::THREE.with_query_version();
}

/// A way of identifying a specific block that has been finalized.
///
/// Useful in contexts that do not work with pending blocks.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BlockId {
    Number(BlockNumber),
    Hash(BlockHash),
    Latest,
}

impl BlockId {
    pub fn is_latest(&self) -> bool {
        self == &Self::Latest
    }
}

impl BlockNumber {
    pub const GENESIS: BlockNumber = BlockNumber::new_or_panic(0);
    /// The maximum [BlockNumber] we can support. Restricted to `u64::MAX/2` to
    /// match Sqlite's maximum integer value.
    pub const MAX: BlockNumber = BlockNumber::new_or_panic(i64::MAX as u64);

    /// Returns the parent's [BlockNumber] or [None] if the current number is
    /// genesis.
    pub fn parent(&self) -> Option<Self> {
        if self == &Self::GENESIS {
            None
        } else {
            Some(*self - 1)
        }
    }

    pub fn is_zero(&self) -> bool {
        self == &Self::GENESIS
    }

    pub fn checked_add(&self, rhs: u64) -> Option<Self> {
        Self::new(self.0.checked_add(rhs)?)
    }

    pub fn checked_sub(&self, rhs: u64) -> Option<Self> {
        self.0.checked_sub(rhs).map(Self)
    }

    pub fn saturating_sub(&self, rhs: u64) -> Self {
        Self(self.0.saturating_sub(rhs))
    }
}

impl std::ops::Add<u64> for BlockNumber {
    type Output = BlockNumber;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl std::ops::AddAssign<u64> for BlockNumber {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl std::ops::Sub<u64> for BlockNumber {
    type Output = BlockNumber;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl std::ops::SubAssign<u64> for BlockNumber {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

/// An Ethereum address.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EthereumAddress(pub H160);

impl<T> Dummy<T> for EthereumAddress {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &T, rng: &mut R) -> Self {
        Self(H160::random_using(rng))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("expected slice length of 16 or less, got {0}")]
pub struct FromSliceError(usize);

impl GasPrice {
    pub const ZERO: GasPrice = GasPrice(0u128);

    /// Returns the big-endian representation of this [GasPrice].
    pub fn to_be_bytes(&self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    /// Constructs [GasPrice] from an array of bytes. Big endian byte order is
    /// assumed.
    pub fn from_be_bytes(src: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(src))
    }

    /// Constructs [GasPrice] from a slice of bytes. Big endian byte order is
    /// assumed.
    pub fn from_be_slice(src: &[u8]) -> Result<Self, FromSliceError> {
        if src.len() > 16 {
            return Err(FromSliceError(src.len()));
        }

        let mut buf = [0u8; 16];
        buf[16 - src.len()..].copy_from_slice(src);

        Ok(Self::from_be_bytes(buf))
    }
}

impl From<u64> for GasPrice {
    fn from(src: u64) -> Self {
        Self(u128::from(src))
    }
}

impl TryFrom<Felt> for GasPrice {
    type Error = anyhow::Error;

    fn try_from(src: Felt) -> Result<Self, Self::Error> {
        anyhow::ensure!(
            src.as_be_bytes()[0..16] == [0; 16],
            "Gas price fits into u128"
        );

        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&src.as_be_bytes()[16..]);
        Ok(Self(u128::from_be_bytes(bytes)))
    }
}

impl From<BlockNumber> for BlockId {
    fn from(number: BlockNumber) -> Self {
        Self::Number(number)
    }
}

impl From<BlockHash> for BlockId {
    fn from(hash: BlockHash) -> Self {
        Self::Hash(hash)
    }
}

/// Ethereum network chains running Starknet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EthereumChain {
    Mainnet,
    Sepolia,
    Other(primitive_types::U256),
}

/// Starknet chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
    Mainnet,
    SepoliaTestnet,
    SepoliaIntegration,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChainId(pub Felt);

impl ChainId {
    /// Convenience function for the constants because unwrap() is not const.
    const fn from_slice_unwrap(slice: &[u8]) -> Self {
        Self(match Felt::from_be_slice(slice) {
            Ok(v) => v,
            Err(_) => panic!("Bad value"),
        })
    }

    /// A hex string representation, eg.: `"0x534e5f4d41494e"` stands for
    /// Mainnet (`SN_MAIN`)
    pub fn to_hex_str(&self) -> std::borrow::Cow<'static, str> {
        self.0.to_hex_str()
    }

    /// A human readable representation, eg.: `"SN_MAIN"` stands for Mainnet
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.0.as_be_bytes())
            .expect("valid utf8")
            .trim_start_matches('\0')
    }

    pub const MAINNET: Self = Self::from_slice_unwrap(b"SN_MAIN");
    pub const SEPOLIA_TESTNET: Self = Self::from_slice_unwrap(b"SN_SEPOLIA");
    pub const SEPOLIA_INTEGRATION: Self = Self::from_slice_unwrap(b"SN_INTEGRATION_SEPOLIA");
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Chain::Mainnet => f.write_str("Mainnet"),
            Chain::SepoliaTestnet => f.write_str("Testnet/Sepolia"),
            Chain::SepoliaIntegration => f.write_str("Integration/Sepolia"),
            Chain::Custom => f.write_str("Custom"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Dummy)]
pub struct StarknetVersion(u8, u8, u8, u8);

impl StarknetVersion {
    pub const fn new(a: u8, b: u8, c: u8, d: u8) -> Self {
        StarknetVersion(a, b, c, d)
    }

    pub fn as_u32(&self) -> u32 {
        u32::from_le_bytes([self.0, self.1, self.2, self.3])
    }

    pub fn from_u32(version: u32) -> Self {
        let [a, b, c, d] = version.to_le_bytes();
        StarknetVersion(a, b, c, d)
    }

    #[inline]
    pub fn major(&self) -> u8 {
        self.0
    }

    #[inline]
    pub fn minor(&self) -> u8 {
        self.1
    }

    #[inline]
    pub fn patch(&self) -> u8 {
        self.2
    }

    pub const V_0_13_2: Self = Self::new(0, 13, 2, 0);

    // TODO: version at which block hash definition changes taken from
    // Starkware implementation but might yet change
    pub const V_0_13_4: Self = Self::new(0, 13, 4, 0);
    // A version at which the state commitment formula changed to always use the
    // Poseidon hash, even when `class_commitment` is zero.
    pub const V_0_14_0: Self = Self::new(0, 14, 0, 0);
    pub const V_0_14_1: Self = Self::new(0, 14, 1, 0);
}

impl FromStr for StarknetVersion {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(StarknetVersion::new(0, 0, 0, 0));
        }

        let parts: Vec<_> = s.split('.').collect();
        anyhow::ensure!(
            parts.len() == 3 || parts.len() == 4,
            "Invalid version string, expected 3 or 4 parts but got {}",
            parts.len()
        );

        let a = parts[0].parse()?;
        let b = parts[1].parse()?;
        let c = parts[2].parse()?;
        let d = parts.get(3).map(|x| x.parse()).transpose()?.unwrap_or(0);

        Ok(StarknetVersion(a, b, c, d))
    }
}

impl Display for StarknetVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 && self.1 == 0 && self.2 == 0 && self.3 == 0 {
            return Ok(());
        }
        if self.3 == 0 {
            write!(f, "{}.{}.{}", self.0, self.1, self.2)
        } else {
            write!(f, "{}.{}.{}.{}", self.0, self.1, self.2, self.3)
        }
    }
}

macros::felt_newtypes!(
    [
        AccountDeploymentDataElem,
        BlockHash,
        ByteCodeOffset,
        BlockCommitmentSignatureElem,
        CallParam,
        CallResultValue,
        ClassCommitment,
        ClassCommitmentLeafHash,
        ConstructorParam,
        ContractAddressSalt,
        ContractNonce,
        ContractStateHash,
        ContractRoot,
        EntryPoint,
        EventCommitment,
        EventData,
        EventKey,
        Fee,
        L1ToL2MessageNonce,
        L1ToL2MessagePayloadElem,
        L2ToL1MessagePayloadElem,
        PaymasterDataElem,
        ProofFactElem,
        ProposalCommitment,
        PublicKey,
        SequencerAddress,
        StateCommitment,
        StateDiffCommitment,
        StorageCommitment,
        StorageValue,
        TransactionCommitment,
        ReceiptCommitment,
        TransactionHash,
        TransactionNonce,
        TransactionSignatureElem,
    ];
    [
        CasmHash,
        ClassHash,
        ContractAddress,
        SierraHash,
        StorageAddress,
    ]
);

macros::fmt::thin_display!(BlockNumber);
macros::fmt::thin_display!(BlockTimestamp);

impl ContractAddress {
    pub fn deployed_contract_address(
        constructor_calldata: impl Iterator<Item = CallParam>,
        contract_address_salt: &ContractAddressSalt,
        class_hash: &ClassHash,
    ) -> Self {
        let constructor_calldata_hash = constructor_calldata
            .fold(HashChain::default(), |mut h, param| {
                h.update(param.0);
                h
            })
            .finalize();

        let contract_address = [
            Felt::from_be_slice(b"STARKNET_CONTRACT_ADDRESS").expect("prefix is convertible"),
            Felt::ZERO,
            contract_address_salt.0,
            class_hash.0,
            constructor_calldata_hash,
        ]
        .into_iter()
        .fold(HashChain::default(), |mut h, e| {
            h.update(e);
            h
        })
        .finalize();

        // Contract addresses are _less than_ 2**251 - 256
        const MAX_CONTRACT_ADDRESS: Felt =
            felt!("0x7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00");
        let contract_address = if contract_address >= MAX_CONTRACT_ADDRESS {
            contract_address - MAX_CONTRACT_ADDRESS
        } else {
            contract_address
        };

        ContractAddress::new_or_panic(contract_address)
    }

    pub fn is_system_contract(&self) -> bool {
        (*self == ContractAddress::ONE) || (*self == ContractAddress::TWO)
    }
}

impl From<ContractAddress> for Vec<u8> {
    fn from(value: ContractAddress) -> Self {
        value.0.to_be_bytes().to_vec()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum AllowedOrigins {
    Any,
    List(Vec<String>),
}

impl<S> From<S> for AllowedOrigins
where
    S: ToString,
{
    fn from(value: S) -> Self {
        let s = value.to_string();

        if s == "*" {
            Self::Any
        } else {
            Self::List(vec![s])
        }
    }
}

/// See:
/// <https://github.com/starkware-libs/cairo-lang/blob/64a7f6aed9757d3d8d6c28bd972df73272b0cb0a/src/starkware/starknet/public/abi.py#L21-L26>
pub fn truncated_keccak(mut plain: [u8; 32]) -> Felt {
    // python code masks with (2**250 - 1) which starts 0x03 and is followed by 31
    // 0xff in be truncation is needed not to overflow the field element.
    plain[0] &= 0x03;
    Felt::from_be_bytes(plain).expect("cannot overflow: smaller than modulus")
}

/// Calculate class commitment tree leaf hash value.
///
/// See: <https://docs.starknet.io/documentation/starknet_versions/upcoming_versions/#state_commitment>
pub fn calculate_class_commitment_leaf_hash(
    compiled_class_hash: CasmHash,
) -> ClassCommitmentLeafHash {
    const CONTRACT_CLASS_HASH_VERSION: pathfinder_crypto::Felt =
        felt_bytes!(b"CONTRACT_CLASS_LEAF_V0");
    ClassCommitmentLeafHash(
        pathfinder_crypto::hash::poseidon_hash(
            CONTRACT_CLASS_HASH_VERSION.into(),
            compiled_class_hash.0.into(),
        )
        .into(),
    )
}

/// A SNOS stwo proof, serialized as a base64-encoded byte string.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Proof(pub Vec<u8>);

impl Proof {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl serde::Serialize for Proof {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use base64::Engine;

        let encoded = base64::engine::general_purpose::STANDARD.encode(&self.0);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> serde::Deserialize<'de> for Proof {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use base64::Engine;

        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(Proof::default());
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)?;
        Ok(Proof(bytes))
    }
}

#[cfg(test)]
mod tests {
    use crate::{felt, CallParam, ClassHash, ContractAddress, ContractAddressSalt};

    #[test]
    fn constructor_entry_point() {
        use sha3::{Digest, Keccak256};

        use crate::{truncated_keccak, EntryPoint};

        let mut keccak = Keccak256::default();
        keccak.update(b"constructor");
        let expected = EntryPoint(truncated_keccak(<[u8; 32]>::from(keccak.finalize())));

        assert_eq!(EntryPoint::CONSTRUCTOR, expected);
    }

    mod starknet_version {
        use std::str::FromStr;

        use super::super::StarknetVersion;

        #[test]
        fn valid_version_parsing() {
            let cases = [
                ("1.2.3.4", "1.2.3.4", StarknetVersion::new(1, 2, 3, 4)),
                ("1.2.3", "1.2.3", StarknetVersion::new(1, 2, 3, 0)),
                ("1.2.3.0", "1.2.3", StarknetVersion::new(1, 2, 3, 0)),
                ("", "", StarknetVersion::new(0, 0, 0, 0)),
            ];

            for (input, output, actual) in cases.iter() {
                let version = StarknetVersion::from_str(input).unwrap();
                assert_eq!(version, *actual);
                assert_eq!(version.to_string(), *output);
            }
        }

        #[test]
        fn invalid_version_parsing() {
            assert!(StarknetVersion::from_str("1.2").is_err());
            assert!(StarknetVersion::from_str("1").is_err());
            assert!(StarknetVersion::from_str("1.2.a").is_err());
        }
    }

    #[test]
    fn deployed_contract_address() {
        let expected_contract_address = ContractAddress(felt!(
            "0x2fab82e4aef1d8664874e1f194951856d48463c3e6bf9a8c68e234a629a6f50"
        ));
        let actual_contract_address = ContractAddress::deployed_contract_address(
            std::iter::once(CallParam(felt!(
                "0x5cd65f3d7daea6c63939d659b8473ea0c5cd81576035a4d34e52fb06840196c"
            ))),
            &ContractAddressSalt(felt!("0x0")),
            &ClassHash(felt!(
                "0x2338634f11772ea342365abd5be9d9dc8a6f44f159ad782fdebd3db5d969738"
            )),
        );
        assert_eq!(actual_contract_address, expected_contract_address);
    }

    mod proof_serde {
        use super::super::Proof;

        #[test]
        fn round_trip() {
            let proof = Proof(vec![0, 0, 0, 0, 0, 0, 0, 123, 0, 0, 1, 200]);
            let json = serde_json::to_string(&proof).unwrap();
            assert_eq!(json, r#""AAAAAAAAAHsAAAHI""#);
            let deserialized: Proof = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, proof);
        }

        #[test]
        fn empty_string_deserializes_to_default() {
            let proof: Proof = serde_json::from_str(r#""""#).unwrap();
            assert_eq!(proof, Proof::default());
        }

        #[test]
        fn invalid_base64_returns_error() {
            let result = serde_json::from_str::<Proof>(r#""not-valid-base64!@#""#);
            assert!(result.is_err());
        }

        #[test]
        fn empty_proof_serializes_to_empty_string() {
            let proof = Proof::default();
            let json = serde_json::to_string(&proof).unwrap();
            assert_eq!(json, r#""""#);
        }
    }
}
