use anyhow::Context;
use pathfinder_common::{BlockId, BlockNumber, BlockTimestamp, SequencerAddress, StarknetVersion};
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
use starknet_gateway_types::reply::{transaction, GasPrices, L1DataAvailabilityMode, Status};

#[serde_as]
#[derive(Debug, Serialize)]
pub struct PreConfirmedPollResponse {
    status: Status,

    #[serde(default)]
    #[serde_as(as = "DisplayFromStr")]
    starknet_version: StarknetVersion,

    l1_da_mode: L1DataAvailabilityMode,

    l1_gas_price: GasPrices,

    l1_data_gas_price: GasPrices,

    #[serde(default)]
    l2_gas_price: Option<GasPrices>,

    timestamp: BlockTimestamp,

    #[serde(default)]
    sequencer_address: Option<SequencerAddress>,

    #[serde_as(as = "Vec<transaction::Transaction>")]
    transactions: Vec<pathfinder_common::transaction::Transaction>,

    #[serde_as(as = "Vec<transaction::Receipt>")]
    transaction_receipts: Vec<(
        pathfinder_common::receipt::Receipt,
        Vec<pathfinder_common::event::Event>,
    )>,

    block_number: BlockNumber,

    block_identifier: String,

    changed: bool,
}

#[tracing::instrument(level = "trace", skip(tx))]
pub fn resolve_preconfirmed_response(
    tx: &pathfinder_storage::Transaction<'_>,
    block_id: BlockId,
    identifier: String,
) -> anyhow::Result<PreConfirmedPollResponse> {
    let header = tx
        .block_header(block_id)
        .context("Fetching block header")?
        .context("Block header missing")?;
    let transactions_receipts = tx
        .transaction_data_for_block(header.number.into())
        .context("Reading transactions from database")?
        .context("Transaction data missing")?;

    let (transactions, transaction_receipts): (Vec<_>, Vec<_>) = transactions_receipts
        .into_iter()
        .map(|(tx, rx, ev)| (tx, (rx, ev)))
        .unzip();

    Ok(PreConfirmedPollResponse {
        status: Status::PreConfirmed,
        starknet_version: header.starknet_version,
        l1_da_mode: header.l1_da_mode.into(),
        l1_gas_price: GasPrices {
            price_in_wei: header.eth_l1_gas_price,
            price_in_fri: header.strk_l1_gas_price,
        },
        l1_data_gas_price: GasPrices {
            price_in_wei: header.eth_l1_data_gas_price,
            price_in_fri: header.strk_l1_data_gas_price,
        },
        l2_gas_price: Some(GasPrices {
            price_in_wei: header.eth_l2_gas_price,
            price_in_fri: header.strk_l2_gas_price,
        }),
        timestamp: header.timestamp,
        sequencer_address: Some(header.sequencer_address),
        transactions,
        transaction_receipts,
        block_number: header.number,
        block_identifier: identifier,
        changed: true,
    })
}
