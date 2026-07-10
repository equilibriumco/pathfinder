#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_traceBlockTransactions against the pre_confirmed block, plain version.
#
# Traces whatever the current pre_confirmed tip is, with no regard for the window
# depth (see the _v1 / _v2 variants for the depth-1 and deep-window cases).
# pre_confirmed is always traced locally; there is no gateway fallback for it.
# Nothing has to be fetched.

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
        "method": "starknet_traceBlockTransactions",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"
