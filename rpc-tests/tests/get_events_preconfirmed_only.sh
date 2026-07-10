#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getEvents over the pre_confirmed block.
#
# With both from_block and to_block set to pre_confirmed, the method queries the
# pre_confirmed block only. No address/key filter, so it returns every event in
# the pre_confirmed block (up to chunk_size). Nothing has to be fetched.

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
        "method": "starknet_getEvents",
        "params": {
                "filter": {
                        "from_block": "pre_confirmed",
                        "to_block": "pre_confirmed",
                        "chunk_size": 100
                }
        }
}' \
"${RPC}"
