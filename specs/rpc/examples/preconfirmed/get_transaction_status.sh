#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getTransactionStatus for a pre_confirmed transaction.
#
# The method has no block_id, but it looks up the transaction in the
# pre_confirmed window first, then the DB, and reports PRE_CONFIRMED for a tx
# that's only in the pre_confirmed block. We take the last transaction hash from
# the current pre_confirmed block and query its status.
#
# Best-effort: by query time the tx may already be committed (then the status is
# ACCEPTED_ON_L2 instead of PRE_CONFIRMED). The pre_confirmed lookup path is what
# we exercise.

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
  "method": "starknet_getTransactionStatus",
  "params": {"transaction_hash": "'"${TX}"'"}
}'
