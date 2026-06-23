use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use blockifier::blockifier::block::pre_process_block;
use blockifier::blockifier::transaction_executor::TransactionExecutor;
use blockifier::blockifier_versioned_constants::VersionedConstants;
use blockifier::bouncer::BouncerConfig;
use blockifier::context::{BlockContext, ChainInfo};
use blockifier::state::cached_state::CachedState;
use pathfinder_common::prelude::*;
use pathfinder_common::L1DataAvailabilityMode;
use starknet_api::block::{BlockHashAndNumber, GasPrice, NonzeroGasPrice};
use starknet_api::core::PatriciaKey;
use starknet_api::versioned_constants_logic::VersionedConstantsTrait;

use super::pending::PendingStateReader;
use super::state_reader::PathfinderStateReader;
use crate::state_reader::{NativeClassCache, StorageAdapter};
use crate::types::BlockInfo;
use crate::IntoStarkFelt;

#[derive(Clone, Debug)]
pub struct VersionedConstantsMap {
    /// Operator-supplied overrides, keyed by the exact Starknet version they
    /// target. Empty unless `--versioned-constants-file` is configured; every
    /// other version resolves to the constants bundled with our blockifier
    /// dependency.
    overrides: BTreeMap<StarknetVersion, VersionedConstants>,
}

impl VersionedConstantsMap {
    pub fn new() -> Self {
        Self {
            overrides: BTreeMap::new(),
        }
    }

    pub fn custom(overrides: BTreeMap<StarknetVersion, VersionedConstants>) -> Self {
        Self { overrides }
    }

    /// The latest Starknet version our blockifier dependency ships constants
    /// for.
    pub fn latest_version() -> StarknetVersion {
        starknet_api::block::StarknetVersion::LATEST
            .to_string()
            .parse()
            .expect("blockifier's latest Starknet version is a valid version string")
    }

    pub fn for_version(&self, version: &StarknetVersion) -> &VersionedConstants {
        // An operator override for this exact version wins.
        if let Some(constants) = self.overrides.get(version) {
            return constants;
        }
        Self::bundled_for_version(version)
    }

    /// Resolves the versioned constants bundled with our blockifier dependency.
    ///
    /// Blockifier ships an exact constants set per released Starknet version
    /// (0.13.0 through [`Self::latest_version`]). For a version it doesn't
    /// recognise we fall back: blocks older than 0.13.0 use 0.13.0, while
    /// blocks newer than this dependency use the latest known constants —
    /// the latter also warns, since execution may be inaccurate until
    /// Pathfinder is upgraded.
    fn bundled_for_version(version: &StarknetVersion) -> &'static VersionedConstants {
        use starknet_api::block::StarknetVersion as ApiVersion;

        if let Some(constants) =
            Self::api_version(version).and_then(|version| VersionedConstants::get(&version).ok())
        {
            return constants;
        }

        if version > &Self::latest_version() {
            tracing::warn!(
                block_version = %version,
                latest_known = %Self::latest_version(),
                "Block's Starknet version is newer than this Pathfinder release supports; \
                 executing with the latest known constants. Upgrade Pathfinder for accurate \
                 execution."
            );
            VersionedConstants::latest_constants()
        } else {
            VersionedConstants::get(&ApiVersion::V0_13_0)
                .expect("blockifier bundles 0.13.0 versioned constants")
        }
    }

    /// Maps a Pathfinder Starknet version onto blockifier's version enum, when
    /// it names a version blockifier knows about.
    fn api_version(version: &StarknetVersion) -> Option<starknet_api::block::StarknetVersion> {
        version.to_string().try_into().ok()
    }
}

impl Default for VersionedConstantsMap {
    fn default() -> Self {
        Self::new()
    }
}

pub type PathfinderExecutor<S> = TransactionExecutor<PendingStateReader<PathfinderStateReader<S>>>;
pub type PathfinderExecutionState<S> = CachedState<PendingStateReader<PathfinderStateReader<S>>>;

pub struct ExecutionState {
    pub chain_id: ChainId,
    pub block_info: BlockInfo,
    execute_on_parent_state: bool,
    pending_state: Option<Arc<StateUpdate>>,
    allow_use_kzg_data: bool,
    versioned_constants_map: VersionedConstantsMap,
    eth_fee_address: ContractAddress,
    strk_fee_address: ContractAddress,
    native_class_cache: Option<NativeClassCache>,
    native_execution_force_use_for_incompatible_classes: bool,
}

pub fn create_executor<S: StorageAdapter + Clone>(
    storage_adapter: S,
    execution_state: ExecutionState,
) -> anyhow::Result<PathfinderExecutor<S>> {
    let config = storage_adapter.transaction_executor_config();

    let StateReaderStage {
        block_context,
        pending_state_reader,
        old_block_number_and_hash,
        ..
    } = execution_state.create_state_reader(storage_adapter)?;

    PathfinderExecutor::pre_process_and_create(
        pending_state_reader,
        block_context,
        old_block_number_and_hash,
        config,
    )
    .context("Preprocessing state and transaction executor")
}

struct StateReaderStage<S: StorageAdapter + Clone> {
    next_block_number: starknet_api::block::BlockNumber,
    block_context: BlockContext,
    pending_state_reader: PendingStateReader<PathfinderStateReader<S>>,
    old_block_number_and_hash: Option<BlockHashAndNumber>,
}

impl ExecutionState {
    pub(super) fn starknet_state<S: StorageAdapter + Clone>(
        self,
        storage_adapter: S,
    ) -> anyhow::Result<(
        CachedState<PendingStateReader<PathfinderStateReader<S>>>,
        BlockContext,
    )> {
        let StateReaderStage {
            next_block_number,
            block_context,
            pending_state_reader,
            old_block_number_and_hash,
        } = self.create_state_reader(storage_adapter)?;

        let mut cached_state = CachedState::new(pending_state_reader);

        pre_process_block(
            &mut cached_state,
            old_block_number_and_hash,
            next_block_number,
            &block_context.versioned_constants().os_constants,
        )?;

        Ok((cached_state, block_context))
    }

    fn create_state_reader<S: StorageAdapter + Clone>(
        self,
        storage_adapter: S,
    ) -> anyhow::Result<StateReaderStage<S>> {
        let block_number = if self.execute_on_parent_state {
            self.block_info.number.parent()
        } else {
            Some(self.block_info.number)
        };

        let chain_info = self.chain_info()?;
        let block_info = self.starknet_block_info()?;

        // Perform system contract updates if we are executing on top of a parent block.
        // Currently this is only the block hash from 10 blocks ago.
        let old_block_number_and_hash = if self.block_info.number.get() >= 10 {
            let block_number_whose_hash_becomes_available =
                pathfinder_common::BlockNumber::new_or_panic(self.block_info.number.get() - 10);

            let block_hash = storage_adapter
                .block_hash(block_number_whose_hash_becomes_available.into())?
                .context(format!(
                    "Getting hash of historical block {block_number_whose_hash_becomes_available}"
                ))?;

            tracing::trace!(%block_number_whose_hash_becomes_available, %block_hash, "Setting historical block hash");

            Some(BlockHashAndNumber {
                number: starknet_api::block::BlockNumber(
                    block_number_whose_hash_becomes_available.get(),
                ),
                hash: starknet_api::block::BlockHash(block_hash.0.into_starkfelt()),
            })
        } else {
            None
        };

        let versioned_constants = self
            .versioned_constants_map
            .for_version(&self.block_info.starknet_version);

        let raw_reader = PathfinderStateReader::new(
            storage_adapter,
            block_number,
            self.pending_state.is_some(),
            self.native_class_cache,
            self.native_execution_force_use_for_incompatible_classes,
        );
        let pending_state_reader = PendingStateReader::new(raw_reader, self.pending_state.clone());

        let next_block_number = block_info.block_number;
        let block_context = BlockContext::new(
            block_info,
            chain_info,
            versioned_constants.clone(),
            BouncerConfig::max(),
        );

        Ok(StateReaderStage {
            next_block_number,
            block_context,
            pending_state_reader,
            old_block_number_and_hash,
        })
    }

    pub(crate) fn chain_info(&self) -> anyhow::Result<ChainInfo> {
        let eth_fee_token_address = starknet_api::core::ContractAddress(
            PatriciaKey::try_from(self.eth_fee_address.0.into_starkfelt())
                .expect("ETH fee token address overflow"),
        );
        let strk_fee_token_address = starknet_api::core::ContractAddress(
            PatriciaKey::try_from(self.strk_fee_address.0.into_starkfelt())
                .expect("STRK fee token address overflow"),
        );

        let chain_id: Vec<_> = self
            .chain_id
            .0
            .to_be_bytes()
            .into_iter()
            .skip_while(|b| *b == 0)
            .collect();
        let chain_id = String::from_utf8(chain_id)?;

        let chain_id = match self.chain_id {
            ChainId::MAINNET => starknet_api::core::ChainId::Mainnet,
            ChainId::SEPOLIA_TESTNET => starknet_api::core::ChainId::Sepolia,
            _ => starknet_api::core::ChainId::Other(chain_id),
        };

        Ok(ChainInfo {
            chain_id,
            fee_token_addresses: blockifier::context::FeeTokenAddresses {
                strk_fee_token_address,
                eth_fee_token_address,
            },
            is_l3: false,
        })
    }

    pub(crate) fn starknet_block_info(&self) -> anyhow::Result<starknet_api::block::BlockInfo> {
        let eth_l1_gas_price =
            NonzeroGasPrice::new(GasPrice(if self.block_info.eth_l1_gas_price.0 == 0 {
                // Bad API design - the genesis block has 0 gas price, but
                // blockifier doesn't allow for it. This isn't critical for
                // consensus, so we just use 1.
                1
            } else {
                self.block_info.eth_l1_gas_price.0
            }))?;
        let strk_l1_gas_price =
            NonzeroGasPrice::new(GasPrice(if self.block_info.strk_l1_gas_price.0 == 0 {
                // Bad API design - the genesis block has 0 gas price, but
                // blockifier doesn't allow for it. This isn't critical for
                // consensus, so we just use 1.
                1
            } else {
                self.block_info.strk_l1_gas_price.0
            }))?;
        let eth_l1_data_gas_price =
            NonzeroGasPrice::new(GasPrice(if self.block_info.eth_l1_data_gas_price.0 == 0 {
                // Bad API design - pre-v0.13.1 blocks have 0 data gas price, but
                // blockifier doesn't allow for it. This value is ignored for those
                // transactions.
                1
            } else {
                self.block_info.eth_l1_data_gas_price.0
            }))?;
        let strk_l1_data_gas_price =
            NonzeroGasPrice::new(GasPrice(if self.block_info.strk_l1_data_gas_price.0 == 0 {
                // Bad API design - pre-v0.13.1 blocks have 0 data gas price, but
                // blockifier doesn't allow for it. This value is ignored for those
                // transactions.
                1
            } else {
                self.block_info.strk_l1_data_gas_price.0
            }))?;
        let eth_l2_gas_price =
            NonzeroGasPrice::new(GasPrice(if self.block_info.eth_l2_gas_price.0 == 0 {
                1
            } else {
                self.block_info.eth_l2_gas_price.0
            }))?;
        let strk_l2_gas_price =
            NonzeroGasPrice::new(GasPrice(if self.block_info.strk_l2_gas_price.0 == 0 {
                1
            } else {
                self.block_info.strk_l2_gas_price.0
            }))?;

        Ok(starknet_api::block::BlockInfo {
            block_number: starknet_api::block::BlockNumber(self.block_info.number.get()),
            block_timestamp: starknet_api::block::BlockTimestamp(self.block_info.timestamp.get()),
            sequencer_address: starknet_api::core::ContractAddress(
                PatriciaKey::try_from(self.block_info.sequencer_address.0.into_starkfelt())
                    .expect("Sequencer address overflow"),
            ),
            gas_prices: starknet_api::block::GasPrices {
                eth_gas_prices: starknet_api::block::GasPriceVector {
                    l1_gas_price: eth_l1_gas_price,
                    l1_data_gas_price: eth_l1_data_gas_price,
                    l2_gas_price: eth_l2_gas_price,
                },
                strk_gas_prices: starknet_api::block::GasPriceVector {
                    l1_gas_price: strk_l1_gas_price,
                    l1_data_gas_price: strk_l1_data_gas_price,
                    l2_gas_price: strk_l2_gas_price,
                },
            },
            use_kzg_da: self.allow_use_kzg_data
                && self.block_info.l1_da_mode == L1DataAvailabilityMode::Blob,
            starknet_version: self
                .block_info
                .starknet_version
                .to_string()
                .try_into()
                .unwrap_or(starknet_api::block::StarknetVersion::PreV0_9_1),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn trace(
        chain_id: ChainId,
        header: BlockHeader,
        pending_state: Option<Arc<StateUpdate>>,
        versioned_constants_map: VersionedConstantsMap,
        eth_fee_address: ContractAddress,
        strk_fee_address: ContractAddress,
        native_class_cache: Option<NativeClassCache>,
        native_execution_force_use_for_incompatible_classes: bool,
    ) -> Self {
        Self {
            chain_id,
            block_info: header.into(),
            pending_state,
            execute_on_parent_state: true,
            allow_use_kzg_data: true,
            versioned_constants_map,
            eth_fee_address,
            strk_fee_address,
            native_class_cache,
            native_execution_force_use_for_incompatible_classes,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn simulation(
        chain_id: ChainId,
        header: BlockHeader,
        pending_state: Option<Arc<StateUpdate>>,
        l1_blob_data_availability: L1BlobDataAvailability,
        versioned_constants_map: VersionedConstantsMap,
        eth_fee_address: ContractAddress,
        strk_fee_address: ContractAddress,
        native_class_cache: Option<NativeClassCache>,
        native_execution_force_use_for_incompatible_classes: bool,
    ) -> Self {
        Self {
            chain_id,
            block_info: header.into(),
            pending_state,
            execute_on_parent_state: false,
            allow_use_kzg_data: l1_blob_data_availability == L1BlobDataAvailability::Enabled,
            versioned_constants_map,
            eth_fee_address,
            strk_fee_address,
            native_class_cache,
            native_execution_force_use_for_incompatible_classes,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn validation(
        chain_id: ChainId,
        block_info: BlockInfo,
        pending_state: Option<Arc<StateUpdate>>,
        versioned_constants_map: VersionedConstantsMap,
        eth_fee_address: ContractAddress,
        strk_fee_address: ContractAddress,
        native_class_cache: Option<NativeClassCache>,
    ) -> Self {
        Self {
            chain_id,
            block_info,
            pending_state,
            execute_on_parent_state: true,
            allow_use_kzg_data: true,
            versioned_constants_map,
            eth_fee_address,
            strk_fee_address,
            native_class_cache,
            native_execution_force_use_for_incompatible_classes: false,
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum L1BlobDataAvailability {
    Disabled,
    Enabled,
}

#[cfg(test)]
mod tests {
    use blockifier::blockifier_versioned_constants::ResourceCost;

    use super::versions::*;
    use super::VersionedConstantsMap;

    #[test]
    fn query_versioned_constants() {
        let vcm = VersionedConstantsMap::default();
        let constants = vcm.for_version(&STARKNET_VERSION_0_13_2);
        let value = constants.deprecated_l2_resource_gas_costs.gas_per_code_byte;
        assert_eq!(value, ResourceCost::new(875, 1000));

        let constants = vcm.for_version(&STARKNET_VERSION_0_13_2_1);
        let value = constants.deprecated_l2_resource_gas_costs.gas_per_code_byte;
        assert_eq!(value, ResourceCost::new(32, 1000));
    }
}
