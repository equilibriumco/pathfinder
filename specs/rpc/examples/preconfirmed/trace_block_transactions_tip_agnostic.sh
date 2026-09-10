#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_traceBlockTransactions against the pre_confirmed block, plain version.
#
# Traces whatever the current pre_confirmed tip is, with no regard for the window
# depth (see the _v1 / _v2 variants for the depth-1 and deep-window cases).
# pre_confirmed is always traced locally; there is no gateway fallback for it.
# Nothing has to be fetched.

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_traceBlockTransactions",
  "params": {"block_id": "pre_confirmed"}
}'
