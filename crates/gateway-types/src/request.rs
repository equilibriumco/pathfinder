//! Structures used for serializing requests to Starkware's sequencer REST API.
pub mod add_transaction {
    use std::collections::HashMap;

    use pathfinder_common::class_definition::{
        EntryPointType,
        SelectorAndFunctionIndex,
        SelectorAndOffset,
    };
    use pathfinder_common::prelude::*;
    use pathfinder_common::{
        CallParam,
        ContractAddress,
        Fee,
        Proof,
        ProofFactElem,
        TransactionSignatureElem,
    };
    use pathfinder_serde::{CallParamAsDecimalStr, TransactionSignatureElemAsDecimalStr};
    use serde_with::serde_as;

    use crate::reply::transaction::{DataAvailabilityMode, ResourceBounds};

    /// Both variants are somewhat different compared to the contract definition
    /// we're using for class hash calculation. The actual program contents
    /// are not relevant for us, and they are sent as a gzip + base64
    /// encoded string via the API.
    #[derive(Clone, Debug, serde::Serialize)]
    #[serde(untagged)]
    pub enum ContractDefinition {
        Cairo(CairoContractDefinition),
        Sierra(SierraContractDefinition),
    }

    /// Definition of a Cairo 0.x contract.
    #[derive(Clone, Debug, serde::Serialize)]
    pub struct CairoContractDefinition {
        // gzip + base64 encoded JSON of the compiled contract JSON
        pub program: String,
        pub entry_points_by_type: HashMap<EntryPointType, Vec<SelectorAndOffset>>,
        pub abi: Option<serde_json::Value>,
    }

    /// Definition of a Cairo 1.x contract.
    #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
    pub struct SierraContractDefinition {
        // gzip + base64 encoded JSON of the compiled contract JSON
        pub sierra_program: String,
        pub contract_class_version: String,
        pub entry_points_by_type: HashMap<EntryPointType, Vec<SelectorAndFunctionIndex>>,
        pub abi: String,
    }

    /// Account deployment transaction details.
    #[derive(Debug, serde::Serialize)]
    #[serde(tag = "version")]
    pub enum DeployAccount<'a> {
        #[serde(rename = "0x0")]
        V0(DeployAccountV0V1<'a>),
        #[serde(rename = "0x1")]
        V1(DeployAccountV0V1<'a>),
        #[serde(rename = "0x3")]
        V3(DeployAccountV3<'a>),
    }

    #[serde_as]
    #[derive(Debug, serde::Serialize)]
    pub struct DeployAccountV0V1<'a> {
        pub max_fee: Fee,
        #[serde_as(as = "&[TransactionSignatureElemAsDecimalStr]")]
        pub signature: &'a [TransactionSignatureElem],
        pub nonce: TransactionNonce,

        pub class_hash: ClassHash,
        pub contract_address_salt: ContractAddressSalt,
        #[serde_as(as = "&[CallParamAsDecimalStr]")]
        pub constructor_calldata: &'a [CallParam],
    }

    #[serde_as]
    #[derive(Debug, serde::Serialize)]
    pub struct DeployAccountV3<'a> {
        pub signature: &'a [TransactionSignatureElem],
        pub nonce: TransactionNonce,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBounds,
        #[serde_as(as = "pathfinder_serde::TipAsHexStr")]
        pub tip: Tip,
        pub paymaster_data: &'a [PaymasterDataElem],

        pub class_hash: ClassHash,
        pub contract_address_salt: ContractAddressSalt,
        pub constructor_calldata: &'a [CallParam],
    }

    /// Invoke contract transaction details.
    #[derive(Debug, serde::Serialize)]
    #[serde(tag = "version")]
    pub enum InvokeFunction<'a> {
        #[serde(rename = "0x0")]
        V0(InvokeFunctionV0V1<'a>),
        #[serde(rename = "0x1")]
        V1(InvokeFunctionV0V1<'a>),
        #[serde(rename = "0x3")]
        V3(InvokeFunctionV3<'a>),
    }

    impl InvokeFunction<'_> {
        pub fn is_proof_empty(&self) -> bool {
            match self {
                InvokeFunction::V0(_) | InvokeFunction::V1(_) => true,
                InvokeFunction::V3(v3) => v3.proof.is_empty(),
            }
        }
    }

    #[serde_as]
    #[derive(Debug, serde::Serialize)]
    pub struct InvokeFunctionV0V1<'a> {
        // AccountTransaction properties
        pub max_fee: Fee,
        pub signature: &'a [TransactionSignatureElem],
        // NOTE: this is optional because Invoke v0 transactions do not have a nonce
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub nonce: Option<TransactionNonce>,

        pub sender_address: ContractAddress,
        // NOTE: this is optional because only Invoke v0 transactions have an entry point selector
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub entry_point_selector: Option<EntryPoint>,
        pub calldata: &'a [CallParam],
    }

    #[serde_as]
    #[derive(Debug, serde::Serialize)]
    pub struct InvokeFunctionV3<'a> {
        pub signature: &'a [TransactionSignatureElem],
        pub nonce: TransactionNonce,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBounds,
        #[serde_as(as = "pathfinder_serde::TipAsHexStr")]
        pub tip: Tip,
        pub paymaster_data: &'a [PaymasterDataElem],

        pub sender_address: ContractAddress,
        pub calldata: &'a [CallParam],
        pub account_deployment_data: &'a [AccountDeploymentDataElem],
        #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
        pub proof_facts: &'a [ProofFactElem],
        #[serde(default, skip_serializing_if = "Proof::is_empty")]
        pub proof: &'a Proof,
    }

    /// Declare transaction details.
    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, serde::Serialize)]
    #[serde(tag = "version")]
    pub enum Declare<'a> {
        #[serde(rename = "0x0")]
        V0(DeclareV0V1V2<'a>),
        #[serde(rename = "0x1")]
        V1(DeclareV0V1V2<'a>),
        #[serde(rename = "0x2")]
        V2(DeclareV0V1V2<'a>),
        #[serde(rename = "0x3")]
        V3(DeclareV3<'a>),
    }

    #[serde_as]
    #[derive(Debug, serde::Serialize)]
    pub struct DeclareV0V1V2<'a> {
        // Transaction properties
        pub version: TransactionVersion,

        // AccountTransaction properties -- except for nonce which is non-optional here
        pub max_fee: Fee,
        pub signature: &'a [TransactionSignatureElem],

        pub contract_class: ContractDefinition,
        pub sender_address: ContractAddress,
        pub nonce: TransactionNonce,

        // Required for declare v2 transactions
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub compiled_class_hash: Option<CasmHash>,
    }

    #[serde_as]
    #[derive(Debug, serde::Serialize)]
    pub struct DeclareV3<'a> {
        pub signature: &'a [TransactionSignatureElem],
        pub nonce: TransactionNonce,
        pub nonce_data_availability_mode: DataAvailabilityMode,
        pub fee_data_availability_mode: DataAvailabilityMode,
        pub resource_bounds: ResourceBounds,
        #[serde_as(as = "pathfinder_serde::TipAsHexStr")]
        pub tip: Tip,
        pub paymaster_data: &'a [PaymasterDataElem],

        pub contract_class: SierraContractDefinition,
        pub compiled_class_hash: CasmHash,
        pub sender_address: ContractAddress,
        pub account_deployment_data: &'a [AccountDeploymentDataElem],
    }

    /// Add transaction API operation.
    ///
    /// This adds the "type" attribute to the JSON request according the type of
    /// the transaction (invoke or deploy).
    #[derive(Debug, serde::Serialize)]
    #[serde(tag = "type")]
    pub enum AddTransaction<'a> {
        #[serde(rename = "INVOKE_FUNCTION")]
        Invoke(InvokeFunction<'a>),
        #[serde(rename = "DECLARE")]
        Declare(Declare<'a>),
        #[serde(rename = "DEPLOY_ACCOUNT")]
        DeployAccount(DeployAccount<'a>),
    }

    #[cfg(test)]
    mod test {
        use starknet_gateway_test_fixtures::add_transaction::INVOKE_CONTRACT_WITH_SIGNATURE;

        use super::*;

        #[test]
        fn test_invoke_with_signature() {
            use pathfinder_common::macro_prelude::*;

            let expected: serde_json::Value =
                serde_json::from_str(INVOKE_CONTRACT_WITH_SIGNATURE).unwrap();

            let input = AddTransaction::Invoke(InvokeFunction::V1(InvokeFunctionV0V1 {
                signature: &[
                    transaction_signature_elem!(
                        "0x7dd3a55d94a0de6f3d6c104d7e6c88ec719a82f4e2bbc12587c8c187584d3d5"
                    ),
                    transaction_signature_elem!(
                        "0x71456dded17015d1234779889d78f3e7c763ddcfd2662b19e7843c7542614f8"
                    ),
                ],
                nonce: Some(transaction_nonce!("0x1")),
                sender_address: contract_address!(
                    "0x23371b227eaecd8e8920cd429357edddd2cd0f3fee6abaacca08d3ab82a7cdd"
                ),
                calldata: &[
                    call_param!("0x1"),
                    call_param!(
                        "0x677bb1cdc050e8d63855e8743ab6e09179138def390676cc03c484daf112ba1"
                    ),
                    call_param!(
                        "0x362398bec32bc0ebb411203221a35a0301193a96f317ebe5e40be9f60d15320"
                    ),
                    call_param!("0x0"),
                    call_param!("0x1"),
                    call_param!("0x1"),
                    call_param!("0x2b"),
                    call_param!("0x0"),
                ],
                entry_point_selector: None,
                max_fee: fee!("0x4f388496839"),
            }));

            assert_eq!(serde_json::to_value(input).unwrap(), expected);
        }

        mod byte_code_offset {
            use pathfinder_common::class_definition::SelectorAndOffset;
            use pathfinder_common::macro_prelude::*;
            use pathfinder_common::ByteCodeOffset;
            use pathfinder_crypto::Felt;

            #[test]
            fn with_hex_offset() {
                let json = r#"{
                    "selector": "0x12345",
                    "offset": "0xabcdef"
                }"#;

                let result = serde_json::from_str::<SelectorAndOffset>(json).unwrap();

                let expected = SelectorAndOffset {
                    selector: entry_point!("0x12345"),
                    offset: byte_code_offset!("0xabcdef"),
                };

                assert_eq!(result, expected);
            }

            #[test]
            fn with_decimal_offset() {
                let json = r#"{
                    "selector": "0x12345",
                    "offset": 199128127
                }"#;

                let result = serde_json::from_str::<SelectorAndOffset>(json).unwrap();

                let expected = SelectorAndOffset {
                    selector: entry_point!("0x12345"),
                    offset: ByteCodeOffset(Felt::from_u64(199128127)),
                };

                assert_eq!(result, expected);
            }
        }
    }
}
