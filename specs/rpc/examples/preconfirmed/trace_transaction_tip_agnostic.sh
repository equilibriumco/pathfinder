#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_traceTransaction for a pre_confirmed transaction, plain version.
#
# Traces the last transaction in the current pre_confirmed block, regardless of
# the window depth (see the _window_size_1 / _window_size_ge_2 variants for the
# empty-overlay vs parents-overlay cases). No block_id: the tx is found in the
# pre_confirmed window and traced locally.
#
# Best-effort: by trace time the tx may already be committed.

# Last transaction hash in the current pre_confirmed block.
TX=$(rpc_call_raw '{"id": 1, "jsonrpc": "2.0", "method": "starknet_getBlockWithTxHashes", "params": {"block_id": "pre_confirmed"}}' | jq -r '.result.transactions[-1] // empty')

if [ -z "${TX}" ]; then
     echo "The pre_confirmed block currently has no transactions; re-run when it does." >&2
     exit 0
fi
echo "Using pre_confirmed transaction: ${TX}"

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_traceTransaction",
  "params": {"transaction_hash": "'"${TX}"'"}
}'
