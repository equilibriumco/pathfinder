#! /usr/bin/env bash
set -e;
set -o pipefail;

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

rpc_call \
'{"jsonrpc":"2.0","id":"0","method":"pathfinder_version"}' \
"http://127.0.0.1:9546/rpc/pathfinder/v0_1"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockWithTxHashes",
        "params": ["pre_confirmed"]
}' \
"http://127.0.0.1:9546/rpc/v0_9"
