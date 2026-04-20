//! Class hash computation for Cairo and Sierra contracts.
//!
//! This crate provides functionality to compute class hashes for both Cairo 0.x
//! and Sierra (Cairo 1.x+) contracts in the Starknet ecosystem. The class hash
//! is a unique identifier for a contract's code that is used throughout the
//! Starknet protocol.
//!
//! # Class Hash Types
//!
//! There are two main types of class hashes:
//!
//! * Cairo 0.x class hashes - Computed for legacy Cairo contracts
//! * Sierra class hashes - Computed for newer Cairo 1.x+ contracts using the
//!   Sierra intermediate representation
//!
//! # Main Components
//!
//! * [`compute_class_hash`] - The main entry point for computing class hashes
//!   for both Cairo and Sierra contracts
//! * [`ComputedClassHash`] - An enum representing the computed hash for either
//!   contract type
//! * [`PreparedCairoContractDefinition`] - A prepared Cairo contract definition
//!   ready for hashing
//! * [`RawCairoContractDefinition`] - An unprepared Cairo contract definition
//!
//! # Implementation Details
//!
//! ## Cairo Class Hash
//!
//! The Cairo class hash computation follows these steps:
//!
//! 1. The contract definition is prepared by removing debug info and handling
//!    special cases for Cairo 0.8+ attributes
//! 2. The prepared definition is serialized to JSON with Python-compatible
//!    formatting
//! 3. A truncated Keccak hash is computed from the serialized JSON
//! 4. Entry points, builtins, and bytecode are processed through hash chains
//! 5. The final class hash is computed by combining all components
//!
//! ## Sierra Class Hash
//!
//! The Sierra class hash computation is simpler:
//!
//! 1. The contract version is validated
//! 2. Entry points are processed in order
//! 3. The ABI string is hashed
//! 4. The Sierra program is hashed
//! 5. All components are combined into the final hash
//!
//! # Compatibility
//!
//! This crate maintains compatibility with the official Starknet implementation
//! and includes extensive test vectors to ensure hash computation matches the
//! network's expectations.
//!
//! See the [official Starknet documentation](https://docs.starknet.io/architecture-and-concepts/smart-contracts/class-hash/)
//! for more details on class hash computation.

use anyhow::{Context, Error, Result};
use pathfinder_common::class_definition::EntryPointType::*;
use pathfinder_common::class_definition::{
    SerializedCairoDefinition,
    SerializedClassDefinition,
    SerializedOpaqueClassDefinition,
    SerializedSierraDefinition,
};
use pathfinder_common::{felt_bytes, ClassHash};
use pathfinder_crypto::hash::{HashChain, PoseidonHasher};
use pathfinder_crypto::Felt;
use serde::Serialize;
use sha3::Digest;

/// Computed class hash
#[derive(Debug, PartialEq)]
pub enum ComputedClassHash {
    Cairo(ClassHash),
    Sierra(ClassHash),
}

impl ComputedClassHash {
    pub fn hash(&self) -> ClassHash {
        match self {
            ComputedClassHash::Cairo(h) => *h,
            ComputedClassHash::Sierra(h) => *h,
        }
    }
}

/// Consumes an opaque serialized class definition and outputs the computed
/// class hash as well as the definition reinterpreted as either a serialized
/// Cairo or Sierra definition.
///
/// This function first parses the JSON blob to decide if it's a Cairo or Sierra
/// class definition and then calls the appropriate function to compute the
/// class hash with the parsed definition.
pub fn compute_class_hash(
    serialized_definition: SerializedOpaqueClassDefinition,
) -> Result<(ComputedClassHash, SerializedClassDefinition)> {
    let contract_definition = parse_contract_definition(&serialized_definition)
        .context("Failed to parse contract definition")?;

    match contract_definition {
        json::ContractDefinition::Sierra(definition) => compute_sierra_class_hash(definition)
            .map(ComputedClassHash::Sierra)
            .context("Compute class hash")
            .map(|hash| {
                (
                    hash,
                    // It is safe to reinterpret the serialized definition as a Sierra definition
                    // since the parsing step succeeded and confirmed it is a
                    // Sierra definition.
                    SerializedClassDefinition::Sierra(SerializedSierraDefinition::from(
                        serialized_definition.into_inner(),
                    )),
                )
            }),
        json::ContractDefinition::Cairo(definition) => compute_cairo_class_hash(definition.into())
            .map(ComputedClassHash::Cairo)
            .context("Compute class hash")
            .map(|hash| {
                (
                    hash,
                    // It is safe to reinterpret the serialized definition as a Cairo definition
                    // since the parsing step succeeded and confirmed it is a Cairo definition.
                    SerializedClassDefinition::Cairo(SerializedCairoDefinition::from(
                        serialized_definition.into_inner(),
                    )),
                )
            }),
    }
}

/// Compute class hash for a Cairo contract definition
pub fn compute_cairo_hinted_class_hash(
    contract_definition: &PreparedCairoContractDefinition<'_>,
) -> Result<Felt> {
    use std::io::Write;

    // It's less efficient than tweaking the formatter to emit the encoding but I
    // don't know how and this is an emergency issue (mainnt nodes stuck).
    let mut string_buffer = vec![];

    let mut ser =
        serde_json::Serializer::with_formatter(&mut string_buffer, PythonDefaultFormatter);

    contract_definition
        .0
        .serialize(&mut ser)
        .context("Serializing contract_definition for Keccak256")?;

    let raw_json_output = String::from_utf8(string_buffer).expect("Invalid UTF-8");

    let mut keccak_writer = KeccakWriter::default();
    keccak_writer
        .write_all(raw_json_output.as_bytes())
        .expect("Failed to write to KeccakWriter");

    let KeccakWriter(hash) = keccak_writer;
    Ok(truncated_keccak(<[u8; 32]>::from(hash.finalize())))
}

/// Parse either a Sierra or a Cairo contract definition.
///
/// Due to an issue in serde_json we can't use an untagged enum and simply
/// derive a Deserialize implementation: <https://github.com/serde-rs/json/issues/559>
fn parse_contract_definition(
    serialized_definition: &SerializedOpaqueClassDefinition,
) -> serde_json::Result<json::ContractDefinition<'_>> {
    serde_json::from_slice::<json::SierraContractDefinition<'_>>(serialized_definition.as_slice())
        .map(json::ContractDefinition::Sierra)
        .or_else(|_| {
            serde_json::from_slice::<json::CairoContractDefinition<'_>>(
                serialized_definition.as_slice(),
            )
            .map(json::ContractDefinition::Cairo)
        })
}

/// Helpers to compute class hashes from the parts that compose a Cairo or
/// Sierra contract
pub mod from_parts {
    use std::collections::HashMap;

    use anyhow::Result;
    use pathfinder_common::class_definition::{
        EntryPointType,
        SelectorAndOffset,
        SierraEntryPoints,
    };
    use pathfinder_common::ClassHash;
    use pathfinder_crypto::Felt;

    use super::json;

    /// Compute class hash from the parts that compose a Cairo contract
    pub fn compute_cairo_class_hash(
        abi: &[u8],
        program: &[u8],
        external_entry_points: Vec<SelectorAndOffset>,
        l1_handler_entry_points: Vec<SelectorAndOffset>,
        constructor_entry_points: Vec<SelectorAndOffset>,
    ) -> Result<ClassHash> {
        let mut entry_points_by_type = HashMap::new();
        entry_points_by_type.insert(EntryPointType::External, external_entry_points);
        entry_points_by_type.insert(EntryPointType::L1Handler, l1_handler_entry_points);
        entry_points_by_type.insert(EntryPointType::Constructor, constructor_entry_points);

        let contract_definition = json::CairoContractDefinition {
            abi: serde_json::from_slice(abi)?,
            program: serde_json::from_slice(program)?,
            entry_points_by_type,
        };

        super::compute_cairo_class_hash(contract_definition.into())
    }

    /// Compute class hash from the parts that compose a Sierra contract
    pub fn compute_sierra_class_hash(
        abi: &str,
        sierra_program: Vec<Felt>,
        contract_class_version: &str,
        entry_points: SierraEntryPoints,
    ) -> Result<ClassHash> {
        let mut entry_points_by_type = HashMap::new();
        entry_points_by_type.insert(EntryPointType::External, entry_points.external);
        entry_points_by_type.insert(EntryPointType::L1Handler, entry_points.l1_handler);
        entry_points_by_type.insert(EntryPointType::Constructor, entry_points.constructor);

        let contract_definition = json::SierraContractDefinition {
            abi: abi.into(),
            sierra_program,
            contract_class_version: contract_class_version.into(),
            entry_points_by_type,
        };

        super::compute_sierra_class_hash(contract_definition)
    }
}

/// An unprepared Cairo contract definition.
///
/// This type represents a raw, unmodified Cairo contract definition before any
/// preprocessing for class hash computation. It serves as a type-safe way to
/// ensure contract definitions go through the proper preparation process.
///
/// # Type Safety
///
/// This type works together with [`PreparedCairoContractDefinition`] to provide
/// compile-time guarantees that contract definitions are properly prepared
/// before being used in class hash computation.
///
/// # Implementation
///
/// Internally wraps a [`json::CairoContractDefinition`] and can be created from
/// one using the [`From`] implementation.
pub struct RawCairoContractDefinition<'a>(json::CairoContractDefinition<'a>);

impl<'a> From<json::CairoContractDefinition<'a>> for RawCairoContractDefinition<'a> {
    fn from(value: json::CairoContractDefinition<'a>) -> Self {
        RawCairoContractDefinition(value)
    }
}

impl<'a> RawCairoContractDefinition<'a> {
    /// Get the inner contract definition
    pub fn inner(&self) -> &json::CairoContractDefinition<'a> {
        &self.0
    }
}

/// A prepared Cairo contract definition ready for class hash computation.
///
/// This type represents a Cairo contract definition that has been preprocessed
/// and is ready for class hash computation. The preparation process includes:
/// - Removal of debug information
/// - Handling of Cairo 0.8+ specific attributes
/// - Proper formatting of named tuple types
///
/// # Type Safety
///
/// This type works together with [`RawCairoContractDefinition`] to provide
/// compile-time guarantees that contract definitions are properly prepared
/// before being used in class hash computation.
///
/// # Creation
///
/// This type can be created from a [`json::CairoContractDefinition`] using
/// [`TryFrom`], which internally uses [`prepare_json_contract_definition`] to
/// ensure all necessary preprocessing steps are applied. The conversion may
/// fail if the contract definition is invalid or cannot be properly prepared.
pub struct PreparedCairoContractDefinition<'a>(json::CairoContractDefinition<'a>);

impl<'a> TryFrom<json::CairoContractDefinition<'a>> for PreparedCairoContractDefinition<'a> {
    type Error = Error;

    fn try_from(value: json::CairoContractDefinition<'a>) -> Result<Self, Self::Error> {
        prepare_json_contract_definition(RawCairoContractDefinition::from(value))
    }
}

impl<'a> PreparedCairoContractDefinition<'a> {
    /// Get the inner contract definition
    pub fn inner(&self) -> &json::CairoContractDefinition<'a> {
        &self.0
    }
}

/// Computes the class hash for given Cairo class definition.
///
/// The structure of the blob is not strictly defined, so it lives in privacy
/// under `json` module of this module. The class hash has [official
/// documentation][starknet-doc] and [cairo-lang
/// has an implementation][cairo-compute] which is half-python and
/// half-[cairo][cairo-contract].
///
/// Outline of the hashing is:
///
/// 1. class definition is serialized with python's [`sort_keys=True`
///    option][py-sortkeys], then a truncated Keccak256 hash is calculated of
///    the serialized json
/// 2. a [hash chain][`HashChain`] construction is used to process in order the
///    contract entry points, builtins, the truncated keccak hash and bytecodes
/// 3. each of the hashchains is hash chained together to produce a final class
///    hash
///
/// Hash chain construction is explained at the [official
/// documentation][starknet-doc], but it's text explanations are much more
/// complex than the actual implementation in `HashChain`.
///
/// [starknet-doc]: https://docs.starknet.io/architecture-and-concepts/smart-contracts/class-hash/
/// [cairo-compute]: https://github.com/starkware-libs/cairo-lang/blob/64a7f6aed9757d3d8d6c28bd972df73272b0cb0a/src/starkware/starknet/core/os/contract_hash.py
/// [cairo-contract]: https://github.com/starkware-libs/cairo-lang/blob/64a7f6aed9757d3d8d6c28bd972df73272b0cb0a/src/starkware/starknet/core/os/contracts.cairo#L76-L118
/// [py-sortkeys]: https://github.com/starkware-libs/cairo-lang/blob/64a7f6aed9757d3d8d6c28bd972df73272b0cb0a/src/starkware/starknet/core/os/contract_hash.py#L58-L71
pub fn compute_cairo_class_hash(
    contract_definition: RawCairoContractDefinition<'_>,
) -> Result<ClassHash> {
    // Prepare the contract definition for class hash computation
    let contract_definition = prepare_json_contract_definition(contract_definition)?;

    // Compute the truncated Keccak hash of the prepared contract definition
    let truncated_keccak = compute_cairo_hinted_class_hash(&contract_definition)?;

    const API_VERSION: Felt = Felt::ZERO;

    let mut outer = HashChain::default();

    // This wasn't in the docs, but similarly to contract_state hash, we start with
    // this 0, so this will yield outer == H(0, 0); However, dissimilarly to
    // contract_state hash, we do include the number of items in this
    // class_hash.
    outer.update(API_VERSION);

    // It is important to process the different entrypoint hashchains in correct
    // order. Each of the entrypoint lists gets updated into the `outer`
    // hashchain.
    //
    // This implementation doesn't preparse the strings, which makes it a bit more
    // noisy. Late parsing is made in an attempt to lean on the one big string
    // allocation we've already got, but these three hash chains could be
    // constructed at deserialization time.
    [External, L1Handler, Constructor]
        .iter()
        .map(|key| {
            contract_definition
                .0
                .entry_points_by_type
                .get(key)
                .unwrap_or(&Vec::new())
                .iter()
                // flatten each entry point to get a list of (selector, offset, selector, offset,
                // ...)
                .flat_map(|x| [x.selector.0, x.offset.0].into_iter())
                .fold(HashChain::default(), |mut hc, next| {
                    hc.update(next);
                    hc
                })
        })
        .for_each(|x| outer.update(x.finalize()));

    fn update_hash_chain(mut hc: HashChain, next: Result<Felt, Error>) -> Result<HashChain, Error> {
        hc.update(next?);
        Result::<_, Error>::Ok(hc)
    }

    let builtins = contract_definition
        .0
        .program
        .builtins
        .iter()
        .enumerate()
        .map(|(i, s)| (i, s.as_bytes()))
        .map(|(i, s)| {
            Felt::from_be_slice(s).with_context(|| format!("Invalid builtin at index {i}"))
        })
        .try_fold(HashChain::default(), update_hash_chain)
        .context("Failed to process contract_definition.program.builtins")?;

    outer.update(builtins.finalize());

    outer.update(truncated_keccak);

    let bytecodes = contract_definition
        .0
        .program
        .data
        .iter()
        .enumerate()
        .map(|(i, s)| {
            Felt::from_hex_str(s).with_context(|| format!("Invalid bytecode at index {i}"))
        })
        .try_fold(HashChain::default(), update_hash_chain)
        .context("Failed to process contract_definition.program.data")?;

    outer.update(bytecodes.finalize());

    Ok(ClassHash(outer.finalize()))
}

/// Prepares a Cairo contract definition for class hash computation by applying
/// necessary transformations.
///
/// This function performs several modifications to ensure compatibility with
/// Starknet's class hash computation:
///
/// 1. Removes the `debug_info` field from the program
/// 2. Handles Cairo 0.8+ specific attribute fields:
///    - Removes empty `accessible_scopes` arrays
///    - Removes `null` `flow_tracking_data` values
/// 3. Ensures proper spacing in named tuple type definitions for older Cairo
///    versions
///
/// # Arguments
///
/// * `contract_definition` - A raw Cairo contract definition to be prepared
///
/// # Returns
///
/// Returns a `Result` containing the prepared contract definition ready for
/// class hash computation, or an error if the preparation process fails.
///
/// # Note
///
/// This preparation step is crucial for maintaining compatibility with the
/// official Starknet implementation and ensuring consistent class hash
/// computation across different Cairo versions.
pub fn prepare_json_contract_definition(
    contract_definition: RawCairoContractDefinition<'_>,
) -> Result<PreparedCairoContractDefinition<'_>, Error> {
    let mut contract_definition = contract_definition.0;
    contract_definition.program.debug_info = None;

    // Cairo 0.8 added "accessible_scopes" and "flow_tracking_data" attribute
    // fields, which were not present in older contracts. They present as null /
    // empty for older contracts and should not be included in the hash
    // calculation in these cases.
    //
    // We therefore check and remove them from the definition before calculating the
    // hash.
    contract_definition
        .program
        .attributes
        .iter_mut()
        .try_for_each(|attr| -> anyhow::Result<()> {
            let vals = attr
                .as_object_mut()
                .context("Program attribute was not an object")?;

            match vals.get_mut("accessible_scopes") {
                Some(serde_json::Value::Array(array)) if array.is_empty() => {
                    vals.remove("accessible_scopes");
                }
                Some(serde_json::Value::Array(_)) | None => {}
                Some(_other) => {
                    anyhow::bail!(
                        r#"A program's attribute["accessible_scopes"] was not an array type."#
                    );
                }
            }
            // We don't know what this type is supposed to be, but if its missing it is
            // null.
            if let Some(serde_json::Value::Null) = vals.get_mut("flow_tracking_data") {
                vals.remove("flow_tracking_data");
            }

            Ok(())
        })?;

    fn add_extra_space_to_cairo_named_tuples(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(v) => walk_array(v),
            serde_json::Value::Object(m) => walk_map(m),
            _ => {}
        }
    }

    fn walk_array(array: &mut [serde_json::Value]) {
        for v in array.iter_mut() {
            add_extra_space_to_cairo_named_tuples(v);
        }
    }

    fn walk_map(object: &mut serde_json::Map<String, serde_json::Value>) {
        for (k, v) in object.iter_mut() {
            match v {
                serde_json::Value::String(s) => {
                    let new_value = add_extra_space_to_named_tuple_type_definition(k, s);
                    if new_value.as_ref() != s {
                        *v = serde_json::Value::String(new_value.into());
                    }
                }
                _ => add_extra_space_to_cairo_named_tuples(v),
            }
        }
    }

    fn add_extra_space_to_named_tuple_type_definition<'a>(
        key: &str,
        value: &'a str,
    ) -> std::borrow::Cow<'a, str> {
        use std::borrow::Cow::*;
        match key {
            "cairo_type" | "value" => Owned(add_extra_space_before_colon(value)),
            _ => Borrowed(value),
        }
    }

    fn add_extra_space_before_colon(v: &str) -> String {
        // This is required because if we receive an already correct ` : `, we will
        // still "repair" it to `  : ` which we then fix at the end.
        v.replace(": ", " : ").replace("  :", " :")
    }

    // Handle a backwards compatibility hack which is required if compiler_version
    // is not present. See `insert_space` for more details.
    if contract_definition.program.compiler_version.is_none() {
        add_extra_space_to_cairo_named_tuples(&mut contract_definition.program.identifiers);
        add_extra_space_to_cairo_named_tuples(&mut contract_definition.program.reference_manager);
    }

    Ok(PreparedCairoContractDefinition(contract_definition))
}

/// Computes the class hash for a Sierra class definition.
///
/// This matches the (not very precise) [official documentation][starknet-doc]
/// and the [cairo-lang implementation][cairo-compute] written in Cairo.
///
/// Calculation is somewhat simpler than for Cairo classes, since it does _not_
/// involve serializing JSON and calculating hashes for the JSON output.
/// Instead, ABI is handled as a string and all other relevant parts of the
/// class definition are transformed into Felts and hashed using Poseidon.
///
/// [starknet-doc]: https://docs.starknet.io/architecture-and-concepts/smart-contracts/class-hash/
/// [cairo-compute]: https://github.com/starkware-libs/cairo-lang/blob/12ca9e91bbdc8a423c63280949c7e34382792067/src/starkware/starknet/core/os/contract_class/contract_class.cairo#L42
pub fn compute_sierra_class_hash(
    contract_definition: json::SierraContractDefinition<'_>,
) -> Result<ClassHash> {
    if contract_definition.contract_class_version != "0.1.0" {
        anyhow::bail!("Unsupported Sierra class version");
    }

    let mut hash = PoseidonHasher::default();

    const SIERRA_VERSION: Felt = felt_bytes!(b"CONTRACT_CLASS_V0.1.0");
    hash.write(SIERRA_VERSION.into());

    // It is important to process the different entrypoint hashchains in correct
    // order. Each of the entrypoint lists gets updated into the `outer`
    // hashchain.
    //
    // This implementation doesn't preparse the strings, which makes it a bit more
    // noisy. Late parsing is made in an attempt to lean on the one big string
    // allocation we've already got, but these three hash chains could be
    // constructed at deserialization time.
    [External, L1Handler, Constructor]
        .iter()
        .map(|key| {
            contract_definition
                .entry_points_by_type
                .get(key)
                .unwrap_or(&Vec::new())
                .iter()
                // flatten each entry point to get a list of (selector, function_idx, selector,
                // function_idx, ...)
                .flat_map(|x| [x.selector.0, x.function_idx.into()].into_iter())
                .fold(PoseidonHasher::default(), |mut hc, next| {
                    hc.write(next.into());
                    hc
                })
        })
        .for_each(|x| hash.write(x.finish()));

    let abi_truncated_keccak = {
        let mut keccak = sha3::Keccak256::default();
        keccak.update(contract_definition.abi.as_bytes());
        truncated_keccak(<[u8; 32]>::from(keccak.finalize()))
    };
    hash.write(abi_truncated_keccak.into());

    let program_hash = {
        let program_hash = contract_definition.sierra_program.iter().fold(
            PoseidonHasher::default(),
            |mut hc, next| {
                hc.write((*next).into());
                hc
            },
        );
        program_hash.finish()
    };
    hash.write(program_hash);

    Ok(ClassHash(hash.finish().into()))
}

/// Computes a truncated Keccak hash compatible with Starknet's field element
/// representation.
///
/// This function takes a 32-byte Keccak hash and truncates it to ensure it fits
/// within Starknet's field element size (251 bits) by masking the most
/// significant bits.
///
/// # Arguments
///
/// * `plain` - A 32-byte array containing the full Keccak hash
///
/// # Returns
///
/// Returns a `Felt` containing the truncated hash value.
///
/// # Implementation Note
///
/// The implementation masks the first byte with 0x03 to ensure the result is
/// less than the Starknet prime field modulus. This matches the official Cairo
/// implementation: <https://github.com/starkware-libs/cairo-lang/blob/64a7f6aed9757d3d8d6c28bd972df73272b0cb0a/src/starkware/starknet/public/abi.py#L21-L26>
pub fn truncated_keccak(mut plain: [u8; 32]) -> Felt {
    // python code masks with (2**250 - 1) which starts 0x03 and is followed by 31
    // 0xff in be truncation is needed not to overflow the field element.
    plain[0] &= 0x03;
    Felt::from_be_bytes(plain).expect("cannot overflow: smaller than modulus")
}

/// `std::io::Write` adapter for Keccak256; we don't need the serialized version
/// in compute_class_hash, but we need the truncated_keccak hash.
///
/// When debugging mismatching hashes, it might be useful to check the length of
/// each before trying to find the wrongly serialized spot. Example length >
/// 500kB.
#[derive(Default)]
struct KeccakWriter(sha3::Keccak256);

impl std::io::Write for KeccakWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // noop is fine, we'll finalize after the write phase
        Ok(())
    }
}

/// Starkware doesn't use compact formatting for JSON but default python
/// formatting. This is required to hash to the same value after sorted
/// serialization.
struct PythonDefaultFormatter;

impl serde_json::ser::Formatter for PythonDefaultFormatter {
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(b": ")
    }

    // Credit: Jonathan Lei from starknet-rs (https://github.com/xJonathanLEI/starknet-rs)`
    #[inline]
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let mut buf = [0, 0];

        for c in fragment.chars() {
            if c.is_ascii() {
                writer.write_all(&[c as u8])?;
            } else {
                let buf = c.encode_utf16(&mut buf);
                for i in buf {
                    write!(writer, r"\u{i:4x}")?;
                }
            }
        }

        Ok(())
    }
}

/// Helpers to parse and serialize the parts that compose a Cairo or Sierra
/// contract
pub mod json {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};

    use pathfinder_common::class_definition::{
        EntryPointType,
        SelectorAndFunctionIndex,
        SelectorAndOffset,
    };

    #[allow(clippy::large_enum_variant)]
    pub enum ContractDefinition<'a> {
        Cairo(CairoContractDefinition<'a>),
        Sierra(SierraContractDefinition<'a>),
    }

    /// A Sierra contract definition
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SierraContractDefinition<'a> {
        /// Contract ABI.
        #[serde(borrow)]
        pub abi: Cow<'a, str>,

        /// Main program definition.
        pub sierra_program: Vec<pathfinder_crypto::Felt>,

        // Version
        #[serde(borrow)]
        pub contract_class_version: Cow<'a, str>,

        /// The contract entry points
        pub entry_points_by_type: HashMap<EntryPointType, Vec<SelectorAndFunctionIndex>>,
    }

    /// Our version of the cairo contract definition used to deserialize and
    /// re-serialize a modified version for a hash of the contract
    /// definition.
    ///
    /// The implementation uses `serde_json::Value` extensively for the
    /// unknown/undefined structure, and the correctness of this
    /// implementation depends on the following features of serde_json:
    ///
    /// - feature `raw_value` has to be enabled for the thrown away
    ///   `program.debug_info`
    /// - feature `preserve_order` has to be disabled, as we want everything
    ///   sorted
    /// - feature `arbitrary_precision` has to be enabled, as there are big
    ///   integers in the input
    ///
    /// It would be much more efficient to have a serde_json::Value which would
    /// only hold borrowed types.
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct CairoContractDefinition<'a> {
        /// Contract ABI, which has no schema definition.
        pub abi: serde_json::Value,

        /// Main program definition.
        #[serde(borrow)]
        pub program: CairoProgram<'a>,

        /// The contract entry points.
        ///
        /// These are left out of the re-serialized version with the ordering
        /// requirement to a Keccak256 hash.
        #[serde(skip_serializing)]
        pub entry_points_by_type: HashMap<EntryPointType, Vec<SelectorAndOffset>>,
    }

    /// A Cairo program definition
    // It's important that this is ordered alphabetically because the fields need to
    // be in sorted order for the keccak hashed representation.
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct CairoProgram<'a> {
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub attributes: Vec<serde_json::Value>,

        #[serde(borrow)]
        pub builtins: Vec<Cow<'a, str>>,

        // Added in Starknet 0.10, so we have to handle this not being present.
        #[serde(borrow, skip_serializing_if = "Option::is_none")]
        pub compiler_version: Option<Cow<'a, str>>,

        #[serde(borrow)]
        pub data: Vec<Cow<'a, str>>,

        #[serde(borrow)]
        pub debug_info: Option<&'a serde_json::value::RawValue>,

        // Important that this is ordered by the numeric keys, not lexicographically
        pub hints: BTreeMap<u64, Vec<serde_json::Value>>,

        pub identifiers: serde_json::Value,

        #[serde(borrow)]
        pub main_scope: Cow<'a, str>,

        // Unlike most other integers, this one is hex string. We don't need to interpret it,
        // it just needs to be part of the hashed output.
        #[serde(borrow)]
        pub prime: Cow<'a, str>,

        pub reference_manager: serde_json::Value,
    }

    #[cfg(test)]
    mod test_vectors {
        use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
        use pathfinder_common::macro_prelude::*;
        use starknet_gateway_test_fixtures::class_definitions::*;

        use super::super::{compute_class_hash, ComputedClassHash};

        fn hash(data: &[u8]) -> ComputedClassHash {
            compute_class_hash(SerializedOpaqueClassDefinition::from_slice(data))
                .unwrap()
                .0
        }

        #[test]
        fn first() {
            assert_eq!(
                hash(INTEGRATION_TEST),
                ComputedClassHash::Cairo(class_hash!(
                    "0x031da92cf5f54bcb81b447e219e2b791b23f3052d12b6c9abd04ff2e5626576"
                ))
            );
        }

        #[test]
        fn second() {
            assert_eq!(
                hash(CONTRACT_DEFINITION),
                ComputedClassHash::Cairo(class_hash!(
                    "0x50b2148c0d782914e0b12a1a32abe5e398930b7e914f82c65cb7afce0a0ab9b"
                ))
            );
        }

        #[test]
        fn genesis_contract() {
            assert_eq!(
                hash(GOERLI_GENESIS),
                ComputedClassHash::Cairo(class_hash!(
                    "0x10455c752b86932ce552f2b0fe81a880746649b9aee7e0d842bf3f52378f9f8"
                ))
            );
        }

        #[test]
        fn cairo_0_8() {
            // Cairo 0.8 update broke our class hash calculation by adding new attribute
            // fields (which we now need to ignore if empty).
            assert_eq!(
                // Known contract which triggered a hash mismatch failure.
                hash(CAIRO_0_8_NEW_ATTRIBUTES),
                ComputedClassHash::Cairo(class_hash!(
                    "0x056b96c1d1bbfa01af44b465763d1b71150fa00c6c9d54c3947f57e979ff68c3"
                ))
            );
        }

        #[test]
        fn cairo_0_10() {
            // Contract whose class triggered a deserialization issue because of the new
            // `compiler_version` property.
            assert_eq!(
                hash(CAIRO_0_10_COMPILER_VERSION),
                ComputedClassHash::Cairo(class_hash!(
                    "0xa69700a89b1fa3648adff91c438b79c75f7dcb0f4798938a144cce221639d6"
                ))
            );
        }

        #[test]
        fn cairo_0_10_part_2() {
            // Contract who's class contains `compiler_version` property as well as
            // `cairo_type` with tuple values. These tuple values require a
            // space to be injected in order to achieve the correct hash.
            assert_eq!(
                hash(CAIRO_0_10_TUPLES_INTEGRATION),
                ComputedClassHash::Cairo(class_hash!(
                    "0x542460935cea188d21e752d8459d82d60497866aaad21f873cbb61621d34f7f"
                ))
            );
        }

        #[test]
        fn cairo_0_10_part_3() {
            // Contract who's class contains `compiler_version` property as well as
            // `cairo_type` with tuple values. These tuple values require a
            // space to be injected in order to achieve the correct hash.
            assert_eq!(
                hash(CAIRO_0_10_TUPLES_GOERLI),
                ComputedClassHash::Cairo(class_hash!(
                    "0x66af14b94491ba4e2aea1117acf0a3155c53d92fdfd9c1f1dcac90dc2d30157"
                ))
            );
        }

        #[test]
        fn cairo_0_11_sierra() {
            assert_eq!(
                hash(CAIRO_0_11_SIERRA),
                ComputedClassHash::Sierra(class_hash!(
                    "0x4e70b19333ae94bd958625f7b61ce9eec631653597e68645e13780061b2136c"
                ))
            )
        }

        #[test]
        fn cairo_0_11_with_decimal_entry_point_offset() {
            assert_eq!(
                hash(CAIRO_0_11_WITH_DECIMAL_ENTRY_POINT_OFFSET),
                ComputedClassHash::Cairo(class_hash!(
                    "0x0484c163658bcce5f9916f486171ac60143a92897533aa7ff7ac800b16c63311"
                ))
            );
        }
    }

    #[cfg(test)]
    mod test_serde_features {
        #[test]
        fn serde_json_value_sorts_maps() {
            // this property is leaned on and the default implementation of serde_json works
            // like this. serde_json has a feature called "preserve_order" which
            // could get enabled by accident, and it would destroy the ability
            // to compute_class_hash.

            let input = r#"{"foo": 1, "bar": 2}"#;
            let parsed = serde_json::from_str::<serde_json::Value>(input).unwrap();
            let output = serde_json::to_string(&parsed).unwrap();

            assert_eq!(output, r#"{"bar":2,"foo":1}"#);
        }

        #[test]
        fn serde_json_has_arbitrary_precision() {
            // the json has 251-bit ints, python handles them out of box, serde_json
            // requires feature "arbitrary_precision".

            // this is 2**256 - 1
            let input = r#"{"foo":115792089237316195423570985008687907853269984665640564039457584007913129639935}"#;

            let output =
                serde_json::to_string(&serde_json::from_str::<serde_json::Value>(input).unwrap())
                    .unwrap();

            assert_eq!(input, output);
        }

        #[test]
        fn serde_json_has_raw_value() {
            // raw value is needed for others but here for completeness; this shouldn't
            // compile if you the feature wasn't enabled.

            #[derive(serde::Deserialize, serde::Serialize)]
            struct Program<'a> {
                #[serde(borrow)]
                debug_info: Option<&'a serde_json::value::RawValue>,
            }

            let mut input = serde_json::from_str::<Program<'_>>(
                r#"{"debug_info": {"long": {"tree": { "which": ["we dont", "care", "about", 0] }}}}"#,
            ).unwrap();

            input.debug_info = None;

            let output = serde_json::to_string(&input).unwrap();

            assert_eq!(output, r#"{"debug_info":null}"#);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn truncated_keccak_matches_pythonic() {
        use pathfinder_common::felt;
        use sha3::{Digest, Keccak256};

        use super::truncated_keccak;
        let all_set = Keccak256::digest([0xffu8; 32]);
        assert!(all_set[0] > 0xf);
        let truncated = truncated_keccak(all_set.into());
        assert_eq!(
            truncated,
            felt!("0x1c584056064687e149968cbab758a3376d22aedc6a55823d1b3ecbee81b8fb9")
        );
    }
}
