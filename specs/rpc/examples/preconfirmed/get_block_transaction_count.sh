#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getBlockTransactionCount against the pre_confirmed block.
#
# Returns the number of transactions in the pre_confirmed block, read straight
# from the pre_confirmed cache. Nothing has to be fetched.

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getBlockTransactionCount",
  "params": {"block_id": "pre_confirmed"}
}'
