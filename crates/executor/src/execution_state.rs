use std::collections::BTreeMap;
use std::num::NonZeroU64;
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

/// A [`BouncerConfig`] whose block capacity cannot be reached when re-executing
/// historical blocks.
///
/// As of blockifier `v0.19.0-rc.2`, [`BouncerConfig::max()`] limits
/// `proving_gas` to `5_000_000_000`, which is a problem for retroactive
/// re-execution of blocks that were built before the `proving_gas` limit was
/// introduced in Starknet 0.14.0.
///
/// Increasing `proving_gas` on its own doesn't help, because the per-builtin
/// proving cost is derived from this budget as `cost = proving_gas /
/// instance_limit`.
///
/// However scaling both the `proving_gas` and every builtin's `instance_limit`
/// by the same factor `K` does work for those blocks, while the cost remains
/// the same: `cost = (K * proving_gas) / (K * instance_limit)`. The limit rises
/// by a factor ok `K`, while the accumulated cost remains the same.
///
/// The fact that for those blocks the limit is now higher is not problematic
/// since they are already part of the blockchain and we only aim at maintaining
/// the ability to re-execute them locally.
fn pre_0_14_0_compatible_bouncer_config(starknet_version: &StarknetVersion) -> BouncerConfig {
    if starknet_version >= &StarknetVersion::V_0_14_0
        || starknet_version < &StarknetVersion::V_0_13_1_1
    {
        return BouncerConfig::max();
    }

    // An arbitrary scaling factor value that works with the affected blocks on
    // sepolia and mainnet.
    const K: u64 = 10;
    let non_zero_mul_by_k = |n: NonZeroU64| {
        NonZeroU64::new(n.get().checked_mul(K).expect("Does not overflow"))
            .expect("Result is nonzero")
    };
    let mut base = BouncerConfig::max();
    base.block_max_capacity.proving_gas =
        starknet_api::execution_resources::GasAmount(base.block_max_capacity.proving_gas.0 * K);
    base.builtin_instance_limits = blockifier::bouncer::BuiltinInstanceLimits {
        pedersen: non_zero_mul_by_k(base.builtin_instance_limits.pedersen),
        range_check: non_zero_mul_by_k(base.builtin_instance_limits.range_check),
        range_check96: non_zero_mul_by_k(base.builtin_instance_limits.range_check96),
        poseidon: non_zero_mul_by_k(base.builtin_instance_limits.poseidon),
        ecdsa: non_zero_mul_by_k(base.builtin_instance_limits.ecdsa),
        ecop: non_zero_mul_by_k(base.builtin_instance_limits.ecop),
        bitwise: non_zero_mul_by_k(base.builtin_instance_limits.bitwise),
        keccak: non_zero_mul_by_k(base.builtin_instance_limits.keccak),
        add_mod: non_zero_mul_by_k(base.builtin_instance_limits.add_mod),
        mul_mod: non_zero_mul_by_k(base.builtin_instance_limits.mul_mod),
        blake: non_zero_mul_by_k(base.builtin_instance_limits.blake),
    };
    base
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
            pre_0_14_0_compatible_bouncer_config(&self.block_info.starknet_version),
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
    use std::collections::BTreeMap;

    use blockifier::blockifier_versioned_constants::VersionedConstants;
    use pathfinder_common::StarknetVersion;
    use starknet_api::block::StarknetVersion as ApiVersion;
    use starknet_api::transaction::fields::ProofVersion;
    use starknet_api::versioned_constants_logic::VersionedConstantsTrait;

    use super::{pre_0_14_0_compatible_bouncer_config, ExecutionState, VersionedConstantsMap};

    fn bundled(version: ApiVersion) -> &'static VersionedConstants {
        VersionedConstants::get(&version).unwrap()
    }

    /// Each released version — including patch releases — resolves to its own
    /// exact bundled constants, never an adjacent version's.
    #[test]
    fn resolves_exact_bundled_version() {
        let vcm = VersionedConstantsMap::default();
        for version in ["0.14.3", "0.13.2.1"] {
            let parsed: StarknetVersion = version.parse().unwrap();
            let api: ApiVersion = version.to_string().try_into().unwrap();
            assert!(
                std::ptr::eq(vcm.for_version(&parsed), bundled(api)),
                "{version} should resolve to its own bundled constants",
            );
        }
    }

    /// Versions outside blockifier's range fall back: older than 0.13.0 to
    /// 0.13.0, newer than our dependency to the latest known constants.
    #[test]
    fn resolves_out_of_range_versions() {
        let vcm = VersionedConstantsMap::default();

        let ancient = "0.12.0".parse().unwrap();
        assert!(std::ptr::eq(
            vcm.for_version(&ancient),
            bundled(ApiVersion::V0_13_0),
        ));

        let future = StarknetVersion::new(0, 99, 0, 0);
        assert!(std::ptr::eq(
            vcm.for_version(&future),
            VersionedConstants::latest_constants(),
        ));
    }

    /// An operator override wins over the bundled constants for its exact
    /// version.
    #[test]
    fn override_takes_precedence() {
        let target: StarknetVersion = "0.14.3".parse().unwrap();
        let proof1 = ProofVersion::V1.as_felt();

        // 0.14.3 normally allows PROOF1...
        assert!(VersionedConstantsMap::default()
            .for_version(&target)
            .os_constants
            .allowed_proof_versions
            .contains(&proof1));

        // ...so overriding it with 0.13.0's constants (which allow no proof
        // versions) proves the override is what gets used.
        let overrides = BTreeMap::from([(target, bundled(ApiVersion::V0_13_0).clone())]);
        assert!(!VersionedConstantsMap::custom(overrides)
            .for_version(&target)
            .os_constants
            .allowed_proof_versions
            .contains(&proof1));
    }

    mod pre_0_14_0_compatible_bouncer_config {
        use blockifier::bouncer::{BouncerConfig, BouncerWeights};
        use pathfinder_common::macro_prelude::*;
        use pathfinder_common::{BlockHeader, BlockNumber, ChainId, StarknetVersion};
        use starknet_api::execution_resources::GasAmount;

        use super::{pre_0_14_0_compatible_bouncer_config, ExecutionState, VersionedConstantsMap};
        use crate::state_reader::RcStorageAdapter;

        fn unscaled_proving_gas_limit_plus_1() -> BouncerWeights {
            let unscaled_proving_gas_ceiling =
                BouncerConfig::max().block_max_capacity.proving_gas.0;
            BouncerWeights {
                proving_gas: GasAmount(unscaled_proving_gas_ceiling + 1),
                ..BouncerWeights::empty()
            }
        }

        fn affected_versions() -> [StarknetVersion; 7] {
            [
                StarknetVersion::V_0_13_1_1,
                StarknetVersion::V_0_13_2,
                StarknetVersion::new(0, 13, 2, 1),
                StarknetVersion::new(0, 13, 3, 0),
                StarknetVersion::V_0_13_4,
                StarknetVersion::new(0, 13, 5, 0),
                StarknetVersion::new(0, 13, 6, 0),
            ]
        }

        #[test]
        fn proving_gas_limit_is_increased_only_where_needed() {
            let over = unscaled_proving_gas_limit_plus_1();

            // The unscaled config
            assert!(!BouncerConfig::max().has_room(over));

            for version in affected_versions() {
                assert!(
                    pre_0_14_0_compatible_bouncer_config(&version).has_room(over),
                    "version: {version}",
                );
            }

            for version in [
                // Below affected range
                StarknetVersion::new(0, 13, 1, 0),
                // Above affected range
                StarknetVersion::V_0_14_0,
                StarknetVersion::V_0_14_1,
                StarknetVersion::new(0, 14, 2, 0),
                StarknetVersion::V_0_14_3,
            ] {
                assert!(
                    !pre_0_14_0_compatible_bouncer_config(&version).has_room(over),
                    "version: {version}",
                );
            }
        }

        #[test]
        fn scaling_preserves_previous_gas_costs() {
            let unscaled = BouncerConfig::max();

            for version in affected_versions() {
                let scaled = pre_0_14_0_compatible_bouncer_config(&version);

                assert_eq!(
                    scaled.builtin_gas_costs(),
                    unscaled.builtin_gas_costs(),
                    "version: {version}",
                );
                assert!(
                    scaled.block_max_capacity.proving_gas > unscaled.block_max_capacity.proving_gas,
                    "version: {version}",
                );
            }
        }

        #[test]
        fn trace_block_context_uses_scaled_bouncer_config() {
            // Below 10 so no historical block hash lookup is needed, the in-memory DB can
            // be empty.
            let block_number = BlockNumber::new_or_panic(5);

            for version in affected_versions() {
                let header = BlockHeader::builder()
                    .number(block_number)
                    .starknet_version(version)
                    .finalize_with_hash(block_hash!("0xabcd"));

                let storage = pathfinder_storage::StorageBuilder::in_memory().unwrap();
                let mut db_conn = storage.connection().unwrap();
                let db_tx = db_conn.transaction().unwrap();

                let state = ExecutionState::trace(
                    ChainId::SEPOLIA_TESTNET,
                    header,
                    None,
                    VersionedConstantsMap::default(),
                    contract_address!("0x1"),
                    contract_address!("0x2"),
                    None,
                    false,
                );

                let stage = state
                    .create_state_reader(RcStorageAdapter::new(db_tx))
                    .unwrap();
                let installed = &stage.block_context.bouncer_config;

                assert!(
                    installed.has_room(unscaled_proving_gas_limit_plus_1()),
                    "{version} should have room above the unscaled limit",
                );
                assert_eq!(
                    installed.builtin_gas_costs(),
                    BouncerConfig::max().builtin_gas_costs(),
                    "{version} should have the same builtin gas costs as the unscaled config",
                );
                assert_eq!(
                    installed,
                    &pre_0_14_0_compatible_bouncer_config(&version),
                    "{version} should use the scaled config",
                );
            }
        }
    }
}
