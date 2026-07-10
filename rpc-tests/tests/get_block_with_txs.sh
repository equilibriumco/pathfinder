#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getBlockWithTxs against the pre_confirmed block.
#
# Returns the pre_confirmed block with full transaction bodies, served from the
# pre_confirmed cache. Nothing has to be fetched.

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
        "method": "starknet_getBlockWithTxs",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"
