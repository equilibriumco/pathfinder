#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getClass against the pre_confirmed block.
#
# Returns a class definition as seen at pre_confirmed. The method first checks
# whether the class is declared in the pre_confirmed data, then serves it from
# the pending overlay or the DB.
#
# getClass needs a class hash, so we fetch one from pathfinder: the class hash
# of the test contract as seen at pre_confirmed (getClassHashAt).

# Class hash of the test contract, as seen at pre_confirmed.
CLASS_HASH=$(rpc_call_raw '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getClassHashAt",
  "params": {
    "block_id": "pre_confirmed",
    "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428"
  }
}' | jq -r '.result')

if [ -z "${CLASS_HASH}" ] || [ "${CLASS_HASH}" = "null" ]; then
     echo "Could not fetch class hash (got: '${CLASS_HASH}')" >&2
     exit 1
fi
echo "Using class hash: ${CLASS_HASH}"

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getClass",
  "params": {
    "block_id": "pre_confirmed",
    "class_hash": "'"${CLASS_HASH}"'"
  }
}'
