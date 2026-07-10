#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getNonce against the pre_confirmed block.
#
# Returns the account's nonce as seen at pre_confirmed: the pre_confirmed data is
# consulted first (so in-flight txs are reflected), falling back to the committed
# nonce. The account address is a fixed input, so nothing has to be fetched.

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
        "method": "starknet_getNonce",
        "params": {
                "block_id": "pre_confirmed",
                "contract_address": "0x3c7e0c59fecffcdfc5c65762bf8c70533de8359497a735a35674eb357f43ff6"
        }
}' \
"${RPC}"
