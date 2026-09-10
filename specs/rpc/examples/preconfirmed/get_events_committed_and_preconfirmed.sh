#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getEvents spanning the committed head into the pre_confirmed block.
#
# from_block = committed head - 1, to_block = pre_confirmed. This is the cross
# boundary case: the method queries the DB for the committed part of the range
# and appends the pre_confirmed block's events on top. No address/key filter, so
# it returns every event in that range (up to chunk_size).
#
# The committed head number is fetched from pathfinder (starknet_blockNumber).

# Committed head, then one below it as the range start.
HEAD=$(rpc_call_raw '{"id": 1, "jsonrpc": "2.0", "method": "starknet_blockNumber"}' | jq -r '.result')

if ! [[ "${HEAD}" =~ ^[0-9]+$ ]]; then
     echo "Could not fetch committed head number (got: '${HEAD}')" >&2
     exit 1
fi
FROM=$((HEAD - 1))
echo "Range: from block_number ${FROM} to pre_confirmed (committed head ${HEAD})"

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getEvents",
  "params": {
    "filter": {
      "from_block": {"block_number": '"${FROM}"'},
      "to_block": "pre_confirmed",
      "chunk_size": 100
    }
  }
}'
