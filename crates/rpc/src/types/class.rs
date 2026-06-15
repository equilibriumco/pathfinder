use anyhow::Context;
use base64::prelude::*;
use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractClass {
    Cairo(cairo::CairoContractClass),
    Sierra(sierra::SierraContractClass),
}

impl ContractClass {
    /// This function behaves in a different way for the variants of
    /// [ContractClass] because of the way the RPC spec treats the
    /// `BROADCASTED_DECLARE_TXN` in `add_declare_transaction`:
    /// - [cairo::CairoContractClass] has its `program` compressed and base64
    ///   encoded, as required by `BROADCASTED_DECLARE_TXN_V1`,
    /// - [sierra::SierraContractClass] does not compress its `sierra_program`
    ///   and represents it as a list of felts, as required by
    ///   `BROADCASTED_DECLARE_TXN_V2`.
    ///
    /// Note that this function does not validate the class definition in any
    /// way, so this is only ever to be called for trusted data from storage.
    pub fn from_serialized_def(
        serialized_definition: &SerializedOpaqueClassDefinition,
    ) -> anyhow::Result<ContractClass> {
        let mut json =
            serde_json::from_slice::<serde_json::Value>(serialized_definition.as_slice())
                .context("Parsing json")?;
        let json_obj = json
            .as_object_mut()
            .context("Class definition is not a json object")?;
        if json_obj.contains_key("sierra_program") {
            Ok(ContractClass::Sierra(
                serde_json::from_value(json).context("Parsing sierra class")?,
            ))
        } else {
            let entry = json_obj
                .get_mut("entry_points_by_type")
                .context("entry_points_by_type property is missing")?
                .take();
            let entry = serde_json::from_value::<cairo::entry_point::ContractEntryPoints>(entry)
                .context("Parsing entry points")?;

            // ABI is optional.
            let abi = json_obj.get_mut("abi").and_then(|json| {
                let json = json.take();
                // ABIs are set by users and not verified by starknet, therefore ABIs
                // can fail to parse (and just be nonsense). Discard these ABIs.
                serde_json::from_value::<Vec<cairo::abi::ContractAbiEntry>>(json).ok()
            });

            let program = json_obj
                .get_mut("program")
                .context("program property is missing")?;

            // Program is expected to be a gzip-compressed then base64 encoded
            // representation of the JSON.
            let mut gzip_encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            serde_json::to_writer(&mut gzip_encoder, &program).context("Compressing program")?;
            let compressed_program = gzip_encoder
                .finish()
                .context("Finalizing program compression")?;
            let encoded_program = BASE64_STANDARD.encode(compressed_program);
            let program = encoded_program;

            Ok(ContractClass::Cairo(cairo::CairoContractClass {
                program,
                entry_points_by_type: entry,
                abi,
            }))
        }
    }

    #[cfg(test)]
    #[allow(clippy::wrong_self_convention)]
    pub fn as_cairo(self) -> Option<cairo::CairoContractClass> {
        match self {
            ContractClass::Cairo(cairo) => Some(cairo),
            ContractClass::Sierra(_) => None,
        }
    }

    #[cfg(test)]
    #[allow(clippy::wrong_self_convention)]
    pub fn as_sierra(self) -> Option<sierra::SierraContractClass> {
        match self {
            ContractClass::Cairo(_) => None,
            ContractClass::Sierra(sierra) => Some(sierra),
        }
    }

    #[cfg(test)]
    pub fn class_hash(&self) -> anyhow::Result<pathfinder_class_hash::ComputedClassHash> {
        match self {
            ContractClass::Cairo(c) => c.class_hash(),
            ContractClass::Sierra(c) => c.class_hash(),
        }
    }
}

impl TryFrom<cairo::CairoContractClass>
    for starknet_gateway_types::request::add_transaction::CairoContractDefinition
{
    type Error = serde_json::Error;

    fn try_from(c: cairo::CairoContractClass) -> Result<Self, Self::Error> {
        use std::collections::HashMap;

        use pathfinder_common::class_definition::{EntryPointType, SelectorAndOffset};

        let abi = match c.abi {
            Some(abi) => Some(serde_json::to_value(abi)?),
            None => None,
        };
        let mut entry_points: HashMap<EntryPointType, Vec<SelectorAndOffset>> = Default::default();
        entry_points.insert(
            EntryPointType::Constructor,
            c.entry_points_by_type
                .constructor
                .into_iter()
                .map(Into::into)
                .collect(),
        );
        entry_points.insert(
            EntryPointType::External,
            c.entry_points_by_type
                .external
                .into_iter()
                .map(Into::into)
                .collect(),
        );
        entry_points.insert(
            EntryPointType::L1Handler,
            c.entry_points_by_type
                .l1_handler
                .into_iter()
                .map(Into::into)
                .collect(),
        );

        Ok(Self {
            program: c.program,
            entry_points_by_type: entry_points,
            abi,
        })
    }
}

impl TryFrom<sierra::SierraContractClass>
    for starknet_gateway_types::request::add_transaction::SierraContractDefinition
{
    type Error = anyhow::Error;

    fn try_from(c: sierra::SierraContractClass) -> Result<Self, Self::Error> {
        use std::collections::HashMap;

        use pathfinder_common::class_definition::{EntryPointType, SelectorAndFunctionIndex};

        let mut entry_points: HashMap<EntryPointType, Vec<SelectorAndFunctionIndex>> =
            Default::default();
        entry_points.insert(
            EntryPointType::Constructor,
            c.entry_points_by_type
                .constructor
                .into_iter()
                .map(Into::into)
                .collect(),
        );
        entry_points.insert(
            EntryPointType::External,
            c.entry_points_by_type
                .external
                .into_iter()
                .map(Into::into)
                .collect(),
        );
        entry_points.insert(
            EntryPointType::L1Handler,
            c.entry_points_by_type
                .l1_handler
                .into_iter()
                .map(Into::into)
                .collect(),
        );

        // Program is expected to be a gzip-compressed then base64 encoded
        // representation of the JSON.
        let mut gzip_encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        serde_json::to_writer(&mut gzip_encoder, &c.sierra_program)
            .context("Compressing program")?;
        let compressed_program = gzip_encoder
            .finish()
            .context("Finalizing program compression")?;
        let encoded_program = BASE64_STANDARD.encode(compressed_program);

        Ok(Self {
            sierra_program: encoded_program,
            contract_class_version: c.contract_class_version,
            entry_points_by_type: entry_points,
            abi: c.abi,
        })
    }
}

pub mod cairo {
    use std::io::{Cursor, Read};

    use anyhow::Context;
    use base64::prelude::*;
    use pathfinder_class_hash::{compute_class_hash, ComputedClassHash};
    use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
    use serde::{Deserialize, Serialize};

    /// A Cairo 0.x class.
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct CairoContractClass {
        // Gzip-compressed then base64-encoded JSON representation of the program.
        pub program: String,
        pub entry_points_by_type: entry_point::ContractEntryPoints,
        pub abi: Option<Vec<abi::ContractAbiEntry>>,
    }

    /// [`CairoContractClass`] is sometimes deserialized as JSON from raw bytes,
    /// so the serde derives are necessary.
    impl crate::dto::DeserializeForVersion for CairoContractClass {
        fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
            value.deserialize_serde()
        }
    }

    impl CairoContractClass {
        pub fn class_hash(&self) -> anyhow::Result<ComputedClassHash> {
            let serialized = self.serialize_to_json()?;
            let definition = SerializedOpaqueClassDefinition::from_vec(serialized);

            compute_class_hash(definition)
                .map(|(hash, _)| hash)
                .context("Compute class hash")
        }

        pub fn serialize_to_json(&self) -> anyhow::Result<Vec<u8>> {
            // decode program
            let decompressor = flate2::read::GzDecoder::new(Cursor::new(
                BASE64_STANDARD.decode(&self.program).unwrap(),
            ));
            let mut program = Vec::new();
            decompressor
                .take(pathfinder_common::class_definition::CLASS_DEFINITION_MAX_ALLOWED_SIZE)
                .read_to_end(&mut program)
                .context("Decompressing program")?;

            let program: program::Program =
                serde_json::from_slice(&program).context("Parsing program JSON")?;

            let json = serde_json::json!({
                "program": program,
                "entry_points_by_type": self.entry_points_by_type,
                "abi": self.abi
            });

            let serialized = serde_json::to_vec(&json)?;

            Ok(serialized)
        }
    }

    /// The `program` property of a Cairo contract class.
    ///
    /// This is a direct mapping of the structure as defined by starknet-rs
    /// taken from <https://github.com/xJonathanLEI/starknet-rs/blob/85906137d634c86b07de20fa33071e5cf186ce21/starknet-core/src/types/contract/legacy.rs>.
    mod program {
        use std::collections::BTreeMap;

        use pathfinder_crypto::Felt;
        use serde::ser::SerializeSeq;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Program {
            #[serde(skip_serializing_if = "Option::is_none")]
            pub attributes: Option<Vec<Attribute>>,
            pub builtins: Vec<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub compiler_version: Option<String>,
            pub data: Vec<Felt>,
            pub debug_info: Option<DebugInfo>,
            pub hints: BTreeMap<u64, Vec<Hint>>,
            pub identifiers: BTreeMap<String, LegacyIdentifier>,
            pub main_scope: String,
            pub prime: String,
            pub reference_manager: ReferenceManager,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Attribute {
            #[serde(default)]
            pub accessible_scopes: Vec<String>,
            pub end_pc: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub flow_tracking_data: Option<FlowTrackingData>,
            pub name: String,
            pub start_pc: u64,
            pub value: String,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct DebugInfo {
            pub file_contents: BTreeMap<String, String>,
            pub instruction_locations: BTreeMap<u64, InstructionLocation>,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Hint {
            pub accessible_scopes: Vec<String>,
            pub code: String,
            pub flow_tracking_data: FlowTrackingData,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct LegacyIdentifier {
            #[serde(skip_serializing_if = "Option::is_none")]
            pub decorators: Option<Vec<String>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub cairo_type: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub full_name: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub members: Option<BTreeMap<String, IdentifierMember>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub references: Option<Vec<Reference>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub size: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub pc: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub destination: Option<String>,
            pub r#type: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub value: Option<Box<serde_json::value::RawValue>>,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ReferenceManager {
            pub references: Vec<Reference>,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct InstructionLocation {
            pub accessible_scopes: Vec<String>,
            pub flow_tracking_data: Option<FlowTrackingData>,
            pub hints: Vec<HintLocation>,
            pub inst: Location,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct IdentifierMember {
            pub cairo_type: String,
            pub offset: u64,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Reference {
            pub ap_tracking_data: ApTrackingData,
            pub pc: u64,
            pub value: String,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct FlowTrackingData {
            pub ap_tracking: ApTrackingData,
            pub reference_ids: BTreeMap<String, u64>,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct HintLocation {
            pub location: Location,
            pub n_prefix_newlines: u64,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Location {
            pub end_col: u64,
            pub end_line: u64,
            pub input_file: InputFile,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub parent_location: Option<ParentLocation>,
            pub start_col: u64,
            pub start_line: u64,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ApTrackingData {
            pub group: u64,
            pub offset: u64,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct InputFile {
            #[serde(skip_serializing_if = "Option::is_none")]
            pub filename: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub content: Option<String>,
        }

        #[derive(Debug, Clone)]
        pub struct ParentLocation {
            pub location: Box<Location>,
            pub remark: String,
        }

        impl Serialize for ParentLocation {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&self.location)?;
                seq.serialize_element(&self.remark)?;
                seq.end()
            }
        }

        impl<'de> Deserialize<'de> for ParentLocation {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct ParentLocationVisitor;

                impl<'de> serde::de::Visitor<'de> for ParentLocationVisitor {
                    type Value = ParentLocation;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str("a (location, remark) pair")
                    }

                    fn visit_seq<S>(self, mut access: S) -> Result<Self::Value, S::Error>
                    where
                        S: serde::de::SeqAccess<'de>,
                    {
                        let location: Location = access
                            .next_element()?
                            .ok_or_else(|| serde::de::Error::missing_field("location"))?;
                        let remark: String = access
                            .next_element()?
                            .ok_or_else(|| serde::de::Error::missing_field("remark"))?;

                        Ok(ParentLocation {
                            location: Box::new(location),
                            remark,
                        })
                    }
                }

                deserializer.deserialize_seq(ParentLocationVisitor)
            }
        }
    }

    pub(crate) mod entry_point {
        use pathfinder_crypto::Felt;
        use pathfinder_serde::U64AsHexStr;
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        #[serde(deny_unknown_fields)]
        pub struct ContractEntryPoints {
            pub constructor: Vec<ContractEntryPoint>,
            pub external: Vec<ContractEntryPoint>,
            pub l1_handler: Vec<ContractEntryPoint>,
        }

        #[serde_with::serde_as]
        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct ContractEntryPoint {
            #[serde_as(as = "OffsetSerde")]
            pub offset: u64,
            #[serde_as(as = "crate::felt::RpcFelt")]
            pub selector: Felt,
        }

        #[derive(serde::Deserialize, serde::Serialize)]
        #[serde(untagged)]
        pub enum OffsetSerde {
            HexStr(U64AsHexStr),
            Decimal(u64),
        }

        impl serde_with::SerializeAs<u64> for OffsetSerde {
            fn serialize_as<S>(source: &u64, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let as_hex = U64AsHexStr(*source);
                as_hex.serialize(serializer)
            }
        }

        impl<'de> serde_with::DeserializeAs<'de, u64> for OffsetSerde {
            fn deserialize_as<D>(deserializer: D) -> Result<u64, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let offset = OffsetSerde::deserialize(deserializer)?;
                let offset = match offset {
                    OffsetSerde::HexStr(wrapped) => wrapped.0,
                    OffsetSerde::Decimal(decimal) => decimal,
                };
                Ok(offset)
            }
        }

        impl From<ContractEntryPoint> for pathfinder_common::class_definition::SelectorAndOffset {
            fn from(entry_point: ContractEntryPoint) -> Self {
                Self {
                    selector: pathfinder_common::EntryPoint(entry_point.selector),
                    offset: pathfinder_common::ByteCodeOffset(Felt::from_u64(entry_point.offset)),
                }
            }
        }
    }

    pub mod abi {
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(untagged)]
        #[serde(deny_unknown_fields)]
        pub enum ContractAbiEntry {
            Function(FunctionAbiEntry),
            Event(EventAbiEntry),
            Struct(StructAbiEntry),
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(rename_all = "lowercase")]
        #[serde(deny_unknown_fields)]
        pub enum StructAbiType {
            Struct,
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(rename_all = "lowercase")]
        #[serde(deny_unknown_fields)]
        pub enum EventAbiType {
            Event,
        }

        #[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        #[serde(deny_unknown_fields)]
        pub enum FunctionAbiType {
            Function,
            L1Handler,
            // This is missing from the v0.2 RPC specification and will be added in the
            // next version. We add it as a deviation from the current spec, since it is
            // effectively a bug in the v0.2 specification.
            Constructor,
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct StructAbiEntry {
            r#type: StructAbiType,
            pub name: String,
            pub size: u64,
            pub members: Vec<StructMember>,
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct StructMember {
            // Serde does not support deny_unknown_fields + flatten, so we
            // flatten TypedParameter manually here.
            #[serde(rename = "name")]
            pub typed_parameter_name: String,
            #[serde(rename = "type")]
            pub typed_parameter_type: String,
            pub offset: u64,
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct EventAbiEntry {
            r#type: EventAbiType,
            pub name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            pub keys: Option<Vec<TypedParameter>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            pub data: Option<Vec<TypedParameter>>,
            // The `inputs` and `outputs` property is not part of the JSON-RPC
            // specification, but because we use these types to parse the
            // `starknet_estimateFee` request and then serialize the class definition in
            // the transaction for the Python layer we have to keep this property when
            // serializing.
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            inputs: Option<Vec<TypedParameter>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            outputs: Option<Vec<TypedParameter>>,
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct FunctionAbiEntry {
            pub r#type: FunctionAbiType,
            pub name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            pub inputs: Option<Vec<TypedParameter>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            pub outputs: Option<Vec<TypedParameter>>,
            // This is not part of the JSON-RPC specification, but because we use these
            // types to parse the `starknet_estimateFee` request and then serialize the
            // class definition in the transaction for the Python layer we have to keep
            // this property when serializing.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            #[serde(rename = "stateMutability")]
            pub state_mutability: Option<String>,
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct TypedParameter {
            pub name: String,
            pub r#type: String,
        }
    }
}

pub mod sierra {
    use pathfinder_class_hash::{compute_class_hash, ComputedClassHash};
    use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
    use pathfinder_crypto::Felt;
    use serde::{Deserialize, Serialize};

    /// A Cairo 1.x (i.e. Sierra) class.
    /// Also matches the gateway representation, which means it
    /// can be used to deserialize directly from storage.
    #[serde_with::serde_as]
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct SierraContractClass {
        #[serde_as(as = "Vec<crate::felt::RpcFelt>")]
        pub sierra_program: Vec<Felt>,
        pub contract_class_version: String,
        pub entry_points_by_type: SierraEntryPoints,
        #[serde(default)]
        pub abi: String,
    }

    /// [`SierraContractClass`] is sometimes deserialized as JSON from raw
    /// bytes, so the serde derives are necessary.
    impl crate::dto::DeserializeForVersion for SierraContractClass {
        fn deserialize(value: crate::dto::Value) -> Result<Self, serde_json::Error> {
            value.deserialize_serde()
        }
    }

    impl<'a> From<SierraContractClass> for pathfinder_common::class_definition::Sierra<'a> {
        fn from(value: SierraContractClass) -> Self {
            Self {
                abi: value.abi.into(),
                sierra_program: value.sierra_program,
                contract_class_version: value.contract_class_version.into(),
                entry_points_by_type: value.entry_points_by_type.into(),
            }
        }
    }

    impl SierraContractClass {
        pub fn class_hash(&self) -> anyhow::Result<ComputedClassHash> {
            let definition = serde_json::to_vec(self)?;
            let definition = SerializedOpaqueClassDefinition::from_vec(definition);
            compute_class_hash(definition).map(|(hash, _)| hash)
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    #[serde(deny_unknown_fields)]
    pub struct SierraEntryPoints {
        pub constructor: Vec<SierraEntryPoint>,
        pub external: Vec<SierraEntryPoint>,
        pub l1_handler: Vec<SierraEntryPoint>,
    }

    impl From<SierraEntryPoints> for pathfinder_common::class_definition::SierraEntryPoints {
        fn from(value: SierraEntryPoints) -> Self {
            let SierraEntryPoints {
                external,
                l1_handler,
                constructor,
            } = value;
            Self {
                external: external.into_iter().map(Into::into).collect(),
                l1_handler: l1_handler.into_iter().map(Into::into).collect(),
                constructor: constructor.into_iter().map(Into::into).collect(),
            }
        }
    }

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct SierraEntryPoint {
        pub function_idx: u64,
        #[serde_as(as = "crate::felt::RpcFelt")]
        pub selector: Felt,
    }

    impl From<SierraEntryPoint> for pathfinder_common::class_definition::SelectorAndFunctionIndex {
        fn from(entry_point: SierraEntryPoint) -> Self {
            Self {
                function_idx: entry_point.function_idx,
                selector: pathfinder_common::EntryPoint(entry_point.selector),
            }
        }
    }
}

#[cfg(test)]
mod tests {

    mod contract_entry_point {
        use pathfinder_common::felt;

        use super::super::cairo::entry_point::ContractEntryPoint;

        #[test]
        fn with_hex_offset() {
            let json = r#"{
                "selector": "0x12345",
                "offset": "0xabcdef"
            }"#;

            let result = serde_json::from_str::<ContractEntryPoint>(json).unwrap();

            let expected = ContractEntryPoint {
                selector: felt!("0x12345"),
                offset: 11259375,
            };

            assert_eq!(result, expected);
        }

        #[test]
        fn with_decimal_offset() {
            let json = r#"{
                "selector": "0x12345",
                "offset": 199128127
            }"#;

            let result = serde_json::from_str::<ContractEntryPoint>(json).unwrap();

            let expected = ContractEntryPoint {
                selector: felt!("0x12345"),
                offset: 199128127,
            };

            assert_eq!(result, expected);
        }
    }

    mod declare_class_hash {
        use pathfinder_class_hash::compute_class_hash;
        use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
        use starknet_gateway_test_fixtures::class_definitions::{
            CAIRO_0_11_SIERRA,
            CONTRACT_DEFINITION,
        };

        use super::super::ContractClass;

        #[test]
        fn compute_sierra_class_hash() {
            let (class_hash, _) = compute_class_hash(SerializedOpaqueClassDefinition::from_slice(
                CAIRO_0_11_SIERRA,
            ))
            .unwrap();

            let class = ContractClass::from_serialized_def(
                &SerializedOpaqueClassDefinition::from_slice(CAIRO_0_11_SIERRA),
            )
            .unwrap();
            assert_eq!(class.class_hash().unwrap(), class_hash);
        }

        #[test]
        fn compute_cairo_class_hash() {
            let (class_hash, _) = compute_class_hash(SerializedOpaqueClassDefinition::from_slice(
                CONTRACT_DEFINITION,
            ))
            .unwrap();

            let class = ContractClass::from_serialized_def(
                &SerializedOpaqueClassDefinition::from_slice(CONTRACT_DEFINITION),
            )
            .unwrap();
            assert_eq!(class.class_hash().unwrap(), class_hash);
        }
    }

    mod contract_class_serialization {
        use pathfinder_common::class_definition::SerializedOpaqueClassDefinition;
        use pathfinder_executor::parse_deprecated_class_definition;

        use super::super::cairo::CairoContractClass;
        use super::super::ContractClass;
        use crate::dto::DeserializeForVersion;

        #[test]
        fn convert_deprecated_class_definition_without_debug_info_into_starknet_api_type() {
            let definition = serde_json::json!({
              "program": "H4sIAAAAAAAC/5WPzQqDMBCE32XPIklPxVcpJURd2wWzCZu1FCTv3qiF9urcZphvf1bwqkL9opihu90b6BealfjrhhgSzSjuhZIpMnRgWntpLTQwevVH60msFVhLAzQiK01U60cQPLHLQ0xYWed26yqdhMIWmffV/Mlac07bJYITCvKAdTz7B0pd/Qv3V0r5AMLJpd3rAAAA",
              "entry_points_by_type": {
                "CONSTRUCTOR":[],
                "EXTERNAL":[],
                "L1_HANDLER":[]
              },
              "abi": []
            });

            let contract_class = CairoContractClass::deserialize(crate::dto::Value::new(
                definition,
                crate::RpcVersion::V09,
            ))
            .unwrap();

            let serialized_definition = contract_class.serialize_to_json().unwrap();

            parse_deprecated_class_definition(SerializedOpaqueClassDefinition::from_vec(
                serialized_definition,
            ))
            .unwrap();
        }

        #[test]
        fn parse_deprecated_class_definition_with_debug_info() {
            let definition =
                include_bytes!("../../fixtures/contracts/cairo0_open_zeppelin_class.json");
            let class = ContractClass::from_serialized_def(
                &SerializedOpaqueClassDefinition::from_slice(definition),
            )
            .unwrap();

            // this step involves parsing the full program including debug info
            class.as_cairo().unwrap().serialize_to_json().unwrap();
        }
    }
}
