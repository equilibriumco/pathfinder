#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_call against the pre_confirmed block.
#
# Calls incr_balance (no args) on the test contract. `call` runs a single entry
# point read-only, without an account or a signature, so no signing is needed.
# The block_id "pre_confirmed" makes it execute on the pre_confirmed state, which
# is what we want to exercise.
#
# No data has to be fetched: the contract is a fixed input and the selector is a
# constant (a pure hash of the method name).
#
#   contract_address       = the block-poke test contract
#   entry_point_selector   = starknet_keccak("incr_balance")

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
        "method": "starknet_call",
        "params": {
                "request": {
                        "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428",
                        "entry_point_selector": "0x5081e27807d3aca1e80c2ea1a743821b452074909f0b650ff7f95e446dfdb2",
                        "calldata": []
                },
                "block_id": "pre_confirmed"
        }
}' \
"${RPC}"
