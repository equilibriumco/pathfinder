#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_estimateFee against the pre_confirmed block.
#
# Estimates the fee for a v3 invoke of incr_balance (no args) sent through the
# account. For PreConfirmed the method runs the transaction on top of the
# pre_confirmed state (pending.aggregated_state_update over the pre_confirmed
# header), which is the pre_confirmed usage we want to exercise.
#
# SKIP_VALIDATE turns off signature validation, so no signing is needed. The
# nonce is still enforced (it must be >= the account nonce), so we fetch the
# account's current nonce at pre_confirmed from pathfinder and splice it in.
#
#   sender_address         = the block-poke test account
#   calldata               = account __execute__, "New" encoding, one call:
#                            [ num_calls=1, to, selector, inner_calldata_len=0 ]
#     to                   = the block-poke test contract
#     selector             = starknet_keccak("incr_balance")

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# Fetch the account's next nonce as seen by the pre_confirmed state.
NONCE=$(curl -s -X POST \
     -H 'Content-Type: application/json' \
     -d '{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getNonce",
        "params": {
                "block_id": "pre_confirmed",
                "contract_address": "0x3c7e0c59fecffcdfc5c65762bf8c70533de8359497a735a35674eb357f43ff6"
        }
     }' \
     "${RPC}" | jq -r '.result')

if [ -z "${NONCE}" ] || [ "${NONCE}" = "null" ]; then
     echo "Could not fetch account nonce (got: '${NONCE}')" >&2
     exit 1
fi
echo "Using account nonce: ${NONCE}"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_estimateFee",
        "params": {
                "request": [
                        {
                                "type": "INVOKE",
                                "version": "0x3",
                                "sender_address": "0x3c7e0c59fecffcdfc5c65762bf8c70533de8359497a735a35674eb357f43ff6",
                                "calldata": [
                                        "0x1",
                                        "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428",
                                        "0x5081e27807d3aca1e80c2ea1a743821b452074909f0b650ff7f95e446dfdb2",
                                        "0x0"
                                ],
                                "signature": [],
                                "nonce": "'"${NONCE}"'",
                                "resource_bounds": {
                                        "l1_gas":      {"max_amount": "0x0", "max_price_per_unit": "0x0"},
                                        "l2_gas":      {"max_amount": "0x0", "max_price_per_unit": "0x0"},
                                        "l1_data_gas": {"max_amount": "0x0", "max_price_per_unit": "0x0"}
                                },
                                "tip": "0x0",
                                "paymaster_data": [],
                                "account_deployment_data": [],
                                "nonce_data_availability_mode": "L1",
                                "fee_data_availability_mode": "L1"
                        }
                ],
                "simulation_flags": ["SKIP_VALIDATE"],
                "block_id": "pre_confirmed"
        }
}' \
"${RPC}"
