#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getBlockWithTxs against the pre_confirmed block.
#
# Returns the pre_confirmed block with full transaction bodies, served from the
# pre_confirmed cache. Nothing has to be fetched.

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getBlockWithTxs",
  "params": {"block_id": "pre_confirmed"}
}'
