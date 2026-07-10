#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getTransactionByBlockIdAndIndex against the pre_confirmed block.
#
# Returns the transaction at a given index in the pre_confirmed block, read from
# the pre_confirmed cache. The pre_confirmed block can be empty, so we first ask
# pathfinder for its transaction count and query the last index (count - 1) when
# there is at least one transaction.
#
# Best-effort: the block keeps growing, so by the time the index query runs more
# transactions may have arrived and count-1 is no longer the actual last one.
# That's fine, the index is still valid.

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# How many transactions are in the pre_confirmed block right now.
COUNT=$(curl -s -X POST \
     -H 'Content-Type: application/json' \
     -d '{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockTransactionCount",
        "params": {"block_id": "pre_confirmed"}
     }' \
     "${RPC}" | jq -r '.result')

if ! [[ "${COUNT}" =~ ^[0-9]+$ ]]; then
     echo "Could not fetch pre_confirmed tx count (got: '${COUNT}')" >&2
     exit 1
fi
if [ "${COUNT}" -eq 0 ]; then
     echo "The pre_confirmed block currently has no transactions; re-run when it does." >&2
     exit 0
fi
INDEX=$((COUNT - 1))
echo "pre_confirmed has ${COUNT} transaction(s); querying last index ${INDEX}"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getTransactionByBlockIdAndIndex",
        "params": {
                "block_id": "pre_confirmed",
                "index": '"${INDEX}"'
        }
}' \
"${RPC}"
