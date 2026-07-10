#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getBlockTransactionCount against the pre_confirmed block.
#
# Returns the number of transactions in the pre_confirmed block, read straight
# from the pre_confirmed cache. Nothing has to be fetched.

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockTransactionCount",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"
