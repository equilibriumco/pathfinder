#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getClassAt against the pre_confirmed block.
#
# Returns the class deployed at the test contract's address, as seen at
# pre_confirmed (pending overlay first, then the DB). The contract address is a
# fixed input, so nothing has to be fetched.

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
        "method": "starknet_getClassAt",
        "params": {
                "block_id": "pre_confirmed",
                "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428"
        }
}' \
"${RPC}"
