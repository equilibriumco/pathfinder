#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_estimateMessageFee against the pre_confirmed block.
#
# Estimates the fee for an L1 -> L2 message (an l1_handler invocation). For
# PreConfirmed the method runs the handler on top of the pre_confirmed state
# (pending.aggregated_state_update over the pre_confirmed header), which is the
# pre_confirmed usage we exercise.
#
# The method needs a contract with an #[l1_handler] entry point. incr_balance is
# a normal external, so we discover an l1_handler on the test contract from its
# class at pre_confirmed (the selector is listed under entry_points_by_type,
# no hashing needed) and target that.
#
# CAVEAT: the payload must match the handler's arguments (the handler receives
# [from_address, ...payload]). We send an empty payload; if the handler takes
# arguments the estimate will fail with a calldata/execution error and you must
# fill `payload` to match its signature.

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# Find an l1_handler selector on the test contract, as seen at pre_confirmed.
SELECTOR=$(curl -s -X POST \
     -H 'Content-Type: application/json' \
     -d '{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getClassAt",
        "params": {
                "block_id": "pre_confirmed",
                "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428"
        }
     }' \
     "${RPC}" | jq -r '.result.entry_points_by_type.L1_HANDLER[0].selector // empty')

if [ -z "${SELECTOR}" ]; then
     echo "The test contract has no l1_handler, so estimateMessageFee can't be exercised against it." >&2
     echo "Point to_address at a contract with an #[l1_handler] and set a matching payload." >&2
     exit 1
fi
echo "Using l1_handler selector: ${SELECTOR}"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_estimateMessageFee",
        "params": {
                "message": {
                        "from_address": "0x0000000000000000000000000000000000000000",
                        "to_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428",
                        "entry_point_selector": "'"${SELECTOR}"'",
                        "payload": []
                },
                "block_id": "pre_confirmed"
        }
}' \
"${RPC}"
