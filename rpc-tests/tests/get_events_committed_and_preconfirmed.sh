#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getEvents spanning the committed head into the pre_confirmed block.
#
# from_block = committed head - 1, to_block = pre_confirmed. This is the cross
# boundary case: the method queries the DB for the committed part of the range
# and appends the pre_confirmed block's events on top. No address/key filter, so
# it returns every event in that range (up to chunk_size).
#
# The committed head number is fetched from pathfinder (starknet_blockNumber).

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# Committed head, then one below it as the range start.
HEAD=$(curl -s -X POST \
     -H 'Content-Type: application/json' \
     -d '{"id": 1, "jsonrpc": "2.0", "method": "starknet_blockNumber"}' \
     "${RPC}" | jq -r '.result')

if ! [[ "${HEAD}" =~ ^[0-9]+$ ]]; then
     echo "Could not fetch committed head number (got: '${HEAD}')" >&2
     exit 1
fi
FROM=$((HEAD - 1))
echo "Range: from block_number ${FROM} to pre_confirmed (committed head ${HEAD})"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getEvents",
        "params": {
                "filter": {
                        "from_block": {"block_number": '"${FROM}"'},
                        "to_block": "pre_confirmed",
                        "chunk_size": 100
                }
        }
}' \
"${RPC}"
