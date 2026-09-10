#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getEvents over the pre_confirmed block.
#
# With both from_block and to_block set to pre_confirmed, the method queries the
# pre_confirmed block only. No address/key filter, so it returns every event in
# the pre_confirmed block (up to chunk_size). Nothing has to be fetched.

rpc_call '{
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
}'
