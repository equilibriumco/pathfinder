macro_rules! str_fixture {
    ($file_name:literal) => {
        include_str!(concat!("../fixtures/", $file_name))
    };
}

macro_rules! bytes_fixture {
    ($file_name:literal) => {
        include_bytes!(concat!("../fixtures/", $file_name))
    };
}

pub mod pre_0_7_0 {
    pub mod block {
        pub const MAINNET_GENESIS: &str = str_fixture!("pre_0.7.0/block/mainnet_genesis.json");
    }
}

pub mod v0_7_0 {
    pub mod block {
        pub const MAINNET_2240: &str = str_fixture!("0.7.0/block/mainnet_2240.json");
    }
}

pub mod v0_8_0 {
    pub mod block {
        pub const MAINNET_2500: &str = str_fixture!("0.8.0/block/mainnet_2500.json");
    }
}

pub mod v0_9_0 {
    pub mod block {
        pub const MAINNET_2800: &str = str_fixture!("0.9.0/block/mainnet_2800.json");
    }
}

pub mod v0_11_1 {
    pub mod block {
        pub const MAINNET_65000: &str = str_fixture!("0.11.1/block/mainnet_65000.json");
    }
}

pub mod v0_12_2 {
    pub mod state_update {
        pub const BLOCK_350000: &str = str_fixture!("0.12.2/state-update/350000.json");
    }
}

pub mod v0_13_1 {
    pub mod state_update_with_block {
        pub const SEPOLIA_INTEGRATION_NUMBER_9703_WITH_SIGNATURE: &str = str_fixture!(
            "0.13.1/state_update_with_block/sepolia_integration_9703_with_signature.json"
        );
        pub const SEPOLIA_INTEGRATION_PENDING: &str =
            str_fixture!("0.13.1/state_update_with_block/sepolia_integration_pending.json");
    }
}

pub mod v0_13_2 {
    pub mod block {
        pub const SEPOLIA_INTEGRATION_35748: &str =
            str_fixture!("0.13.2/block/sepolia_integration_35748.json");
    }

    pub mod signature {
        pub const SEPOLIA_INTEGRATION_35748: &str =
            str_fixture!("0.13.2/signature/sepolia_integration_35748.json");
    }

    pub mod state_update {
        pub const SEPOLIA_INTEGRATION_35748: &str =
            str_fixture!("0.13.2/state_update/sepolia_integration_35748.json");
    }
}

pub mod v0_13_4 {
    pub mod block {
        pub const SEPOLIA_INTEGRATION_63881: &str =
            str_fixture!("0.13.4/block/sepolia_integration_63881.json");
    }

    pub mod state_update {
        pub const SEPOLIA_INTEGRATION_63881: &str =
            str_fixture!("0.13.4/state_update/sepolia_integration_63881.json");
    }

    pub mod traces {
        pub const SEPOLIA_TESTNET_30000: &str =
            str_fixture!("0.13.4/traces/sepolia_testnet_30000.json");
    }
}

pub mod v0_14_0 {
    pub mod preconfirmed_block {
        pub const SEPOLIA_INTEGRATION_955821: &str =
            str_fixture!("0.14.0/preconfirmed_block/sepolia_integration_955821.json");
    }
}

pub mod v0_14_1 {
    pub mod state_update_with_block {
        pub const SEPOLIA_INTEGRATION_3077642_WITH_SIGNATURE: &str =
            str_fixture!("0.14.1/state_update/sepolia_integration_3077642_with_signature.json");
    }
}

pub mod v0_14_3 {
    pub mod state_update_with_block {
        pub const SEPOLIA_INTEGRATION_FAKE_WITH_SIGNATURE: &str =
            str_fixture!("0.14.3/state_update/sepolia_integration_fake_with_signature.json");
    }
}

pub mod add_transaction {
    pub const INVOKE_CONTRACT_WITH_SIGNATURE: &str =
        str_fixture!("add-transaction/invoke-contract-with-signature.json");
}

pub mod class_definitions {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::ClassHash;

    pub const CONTRACT_DEFINITION: &[u8] = bytes_fixture!("contracts/contract_definition.json");
    pub const CONTRACT_DEFINITION_CLASS_HASH: ClassHash =
        class_hash!("0x050b2148c0d782914e0b12a1a32abe5e398930b7e914f82c65cb7afce0a0ab9b");
    pub const DUMMY_ACCOUNT: &[u8] = bytes_fixture!("contracts/dummy_account.json");
    pub const DUMMY_ACCOUNT_CLASS_HASH: ClassHash =
        class_hash!("0x0791563da22895f1e398b689866718346106c0cc71207a4ada68e6687ce1badf");

    // A "dummy" ERC20 contract that can be used as a fee token contract
    // https://github.com/starkware-libs/blockifier/blob/3c8ee7f541db035b49fcfb203aa85f8b0b6b42e5/crates/blockifier/ERC20_without_some_syscalls/ERC20/erc20_contract_without_some_syscalls_compiled.json
    pub const ERC20_CONTRACT_DEFINITION: &[u8] = bytes_fixture!("contracts/erc20_contract.json");
    pub const ERC20_CONTRACT_DEFINITION_CLASS_HASH: ClassHash =
        class_hash!("0x013dbe991273192b5573c526cddc27a27decb8525b44536cb0f57b5b2c089b51");

    // https://external.integration.starknet.io/feeder_gateway/get_full_contract?blockNumber=latest&contractAddress=0x4ae0618c330c59559a59a27d143dd1c07cd74cf4e5e5a7cd85d53c6bf0e89dc
    pub const INTEGRATION_TEST: &[u8] = bytes_fixture!("contracts/integration-test.json");
    // https://alpha4.starknet.io/feeder_gateway/get_full_contract?contractAddress=0546BA9763D33DC59A070C0D87D94F2DCAFA82C4A93B5E2BF5AE458B0013A9D3
    pub const GOERLI_GENESIS: &[u8] = bytes_fixture!("contracts/goerli-genesis.json");
    // https://alpha4.starknet.io/feeder_gateway/get_full_contract?contractAddress=0400D86342F474F14AAE562587F30855E127AD661F31793C49414228B54516EC
    pub const CAIRO_0_8_NEW_ATTRIBUTES: &[u8] =
        bytes_fixture!("contracts/cairo-0.8-new-attributes.json");
    // Contract whose class triggered a deserialization issue because of the new
    // `compiler_version` property. https://external.integration.starknet.io/feeder_gateway/get_full_contract?blockNumber=latest&contractAddress=0x444453070729bf2db6a1f36541483c2952674e5de4bd05fcf538726b286bfa2
    pub const CAIRO_0_10_COMPILER_VERSION: &[u8] =
        bytes_fixture!("contracts/cairo-0.10-compiler-version.json");
    // Contracts whose class contains `compiler_version` property as well as
    // `cairo_type` with tuple values. These tuple values require a space to be
    // injected in order to achieve the correct hash. https://external.integration.starknet.io/feeder_gateway/get_full_contract?blockNumber=latest&contractAddress=0x06f17fb7a052f3d18c1911c9d9c2fb0032bbe1ea57c58b0baca85bda9f3698be
    pub const CAIRO_0_10_TUPLES_INTEGRATION: &[u8] =
        bytes_fixture!("contracts/cairo-0.10-tuples-integration.json");
    // https://alpha4.starknet.io/feeder_gateway/get_full_contract?blockNumber=latest&contractAddress=0x0424e799d610433168a31aab44c0d3e38b45d97387b45de80089f56c184fa315
    pub const CAIRO_0_10_TUPLES_GOERLI: &[u8] =
        bytes_fixture!("contracts/cairo-0.10-tuples-goerli.json");
    // https://external.integration.starknet.io/feeder_gateway/get_class_by_hash?classHash=0x4e70b19333ae94bd958625f7b61ce9eec631653597e68645e13780061b2136c
    pub const CAIRO_0_11_SIERRA: &[u8] = bytes_fixture!("contracts/sierra-0.11.json");
    // https://github.com/starkware-libs/cairo/blob/v1.0.0-alpha.5/crates/cairo-lang-starknet/test_data/test_contract.json, but slightly
    // modified: "abi" has been converted to a string and debug info is removed
    pub const CAIRO_1_0_0_ALPHA5_SIERRA: &[u8] =
        bytes_fixture!("contracts/sierra-1.0.0.alpha5-starknet-format.json");
    // https://external.integration.starknet.io/feeder_gateway/get_class_by_hash?classHash=0x4d7d2ddf396736d7cdba26e178e30e3388d488984a94e03bc4af4841e222920
    pub const CAIRO_1_0_0_ALPHA6_SIERRA: &[u8] =
        bytes_fixture!("contracts/sierra-1.0.0.alpha6.json");
    // https://external.integration.starknet.io/feeder_gateway/get_class_by_hash?classHash=0x0484c163658bcce5f9916f486171ac60143a92897533aa7ff7ac800b16c63311
    pub const CAIRO_0_11_WITH_DECIMAL_ENTRY_POINT_OFFSET: &[u8] =
        bytes_fixture!("contracts/cairo-0.11.0-decimal-entry-point-offset.json");

    // A Sierra class with the program compression introduced in v0.11.1.
    // https://external.integration.starknet.io/feeder_gateway/get_class_by_hash?classHash=0x05bb6c878494878bda6c2f0d7605f66559f9ffd6ae69ff529f8ca5f7a587a2bb
    pub const CAIRO_1_0_0_RC0_SIERRA: &[u8] = bytes_fixture!("contracts/sierra-1.0.0.rc0.json");

    // A Sierra class for compiler v1.1.0-rc0 introduced in v0.11.2.
    // https://external.integration.starknet.io/feeder_gateway/get_class_by_hash?classHash=0x1338d85d3e579f6944ba06c005238d145920afeb32f94e3a1e234d21e1e9292
    pub const CAIRO_1_1_0_RC0_SIERRA: &[u8] = bytes_fixture!("contracts/sierra-1.1.0.rc0.json");

    // https://testnet.starkscan.co/contract/0x04b4e6d2b66287bd98f6c46daff06cba10942c7d7fe517825f0d3761cac36225
    pub const CAIRO_1_1_0_BALANCE_SIERRA_JSON: &[u8] =
        bytes_fixture!("contracts/sierra-1.1.0-balance.json");
    pub const CAIRO_1_1_0_BALANCE_CASM_JSON: &[u8] =
        bytes_fixture!("contracts/sierra-1.1.0-balance.casm.json");

    // A sierra class which caused a stack overflow in the 2.0.1 compiler.
    // https://alpha4.starknet.io/feeder_gateway/get_class_by_hash?classHash=0x03dd9347d22f1ea2d5fbc7bd1f0860c6c334973499f9f1989fcb81bfff5191da
    pub const CAIRO_2_0_0_STACK_OVERFLOW: &[u8] =
        bytes_fixture!("contracts/sierra-2.0.0-stack-overflow.json");

    // A Cairo class from Testnet
    pub const CAIRO_TESTNET_0331118F4E4EB8A8DDB0F4493E09612E380EF527991C49A15C42574AB48DD747:
        &[u8] = bytes_fixture!(
        "contracts/cairo-testnet-0331118f4e4eb8a8ddb0f4493e09612e380ef527991c49a15c42574ab48dd747.\
         json"
    );
    pub const CAIRO_TESTNET_0331118F4E4EB8A8DDB0F4493E09612E380EF527991C49A15C42574AB48DD747_CLASS_HASH: ClassHash =
        class_hash!("0x0331118f4e4eb8a8ddb0f4493e09612e380ef527991c49a15c42574ab48dd747");

    // A Sierra class from Testnet
    pub const SIERRA_TESTNET_02E62A7336B45FA98668A6275168CE42B085665A9EC16B100D895968691A0BDC: &[u8] =
        bytes_fixture!("contracts/sierra-testnet-02e62a7336b45fa98668a6275168ce42b085665a9ec16b100d895968691a0bdc.json");
    pub const SIERRA_TESTNET_02E62A7336B45FA98668A6275168CE42B085665A9EC16B100D895968691A0BDC_CLASS_HASH: ClassHash =
        class_hash!("0x02e62a7336b45fa98668a6275168ce42b085665a9ec16b100d895968691a0bdc");
}

pub mod testnet {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_crypto::Felt;

    pub const VALID_TX_HASH: TransactionHash =
        transaction_hash!("0x0493d8fab73af67e972788e603aee18130facd3c7685f16084ecd98b07153e24");
    pub const INVALID_TX_HASH: TransactionHash =
        transaction_hash!("0x0393d8fab73af67e972788e603aee18130facd3c7685f16084ecd98b07153e24");
    pub const VALID_CONTRACT_ADDR: ContractAddress =
        contract_address!("0x06fbd460228d843b7fbef670ff15607bf72e19fa94de21e29811ada167b4ca39");
    pub const INVALID_CONTRACT_ADDR: ContractAddress =
        contract_address!("0x05fbd460228d843b7fbef670ff15607bf72e19fa94de21e29811ada167b4ca39");
    pub const VALID_ENTRY_POINT: EntryPoint =
        entry_point!("0x0362398bec32bc0ebb411203221a35a0301193a96f317ebe5e40be9f60d15320");
    pub const INVALID_ENTRY_POINT: EntryPoint = EntryPoint(Felt::ZERO);
    pub const VALID_KEY: StorageAddress =
        storage_address!("0x0206F38F7E4F15E87567361213C28F235CCCDAA1D7FD34C9DB1DFE9489C6A091");
    pub const VALID_KEY_DEC: &str =
        "916907772491729262376534102982219947830828984996257231353398618781993312401";
    pub const VALID_CALL_DATA: [CallParam; 1] = [call_param!("0x4d2")];
    /// Class hash for VALID_CONTRACT_ADDR
    pub const VALID_CLASS_HASH: ClassHash =
        class_hash!("0x021a7f43387573b68666669a0ed764252ce5367708e696e31967764a90b429c2");
    pub const INVALID_CLASS_HASH: ClassHash =
        class_hash!("0x031a7f43387573b68666669a0ed764252ce5367708e696e31967764a90b429c2");
}

pub mod traces {
    pub const TESTNET_GENESIS: &[u8] = bytes_fixture!("traces/block_testnet_0.json");
    pub const TESTNET_889_517: &[u8] = bytes_fixture!("traces/block_testnet_889_517.json");

    pub const TESTNET_TX_0_0: &[u8] = bytes_fixture!("traces/transaction_testnet_0_0.json");
    pub const TESTNET_TX_899_517_0: &[u8] =
        bytes_fixture!("traces/transaction_testnet_889_517_0.json");
    // full tx hash is
    // 0x6a4a9c4f1a530f7d6dd7bba9b71f090a70d1e3bbde80998fde11a08aab8b282
    pub const SEPOLIA_TESTNET_TX_0X6A4A: &[u8] =
        bytes_fixture!("traces/transaction_sepolia_testnet_0x6a4a.json");
}
