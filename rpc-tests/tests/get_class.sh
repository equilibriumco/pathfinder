#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getClass against the pre_confirmed block.
#
# Returns a class definition as seen at pre_confirmed. The method first checks
# whether the class is declared in the pre_confirmed data, then serves it from
# the pending overlay or the DB.
#
# getClass needs a class hash, so we fetch one from pathfinder: the class hash
# of the test contract as seen at pre_confirmed (getClassHashAt).

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# Class hash of the test contract, as seen at pre_confirmed.
CLASS_HASH=$(curl -s -X POST \
     -H 'Content-Type: application/json' \
     -d '{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getClassHashAt",
        "params": {
                "block_id": "pre_confirmed",
                "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428"
        }
     }' \
     "${RPC}" | jq -r '.result')

if [ -z "${CLASS_HASH}" ] || [ "${CLASS_HASH}" = "null" ]; then
     echo "Could not fetch class hash (got: '${CLASS_HASH}')" >&2
     exit 1
fi
echo "Using class hash: ${CLASS_HASH}"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getClass",
        "params": {
                "block_id": "pre_confirmed",
                "class_hash": "'"${CLASS_HASH}"'"
        }
}' \
"${RPC}"
