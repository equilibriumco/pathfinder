#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_getTransactionByHash for a pre_confirmed transaction.
#
# The method has no block_id, but it looks up the transaction in the
# pre_confirmed window first, then the DB. We take the last transaction hash from
# the current pre_confirmed block (the most recently added, most likely still
# only pre_confirmed) and query it.
#
# Best-effort: the block grows and commits, so by query time the tx may already
# be committed. It still resolves (getTransactionByHash finds committed txs too),
# the pre_confirmed lookup path is what we exercise.

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# Last transaction hash in the current pre_confirmed block.
TX=$(curl -s -X POST \
     -H 'Content-Type: application/json' \
     -d '{"id": 1, "jsonrpc": "2.0", "method": "starknet_getBlockWithTxHashes", "params": {"block_id": "pre_confirmed"}}' \
     "${RPC}" | jq -r '.result.transactions[-1] // empty')

if [ -z "${TX}" ]; then
     echo "The pre_confirmed block currently has no transactions; re-run when it does." >&2
     exit 0
fi
echo "Using pre_confirmed transaction: ${TX}"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_getTransactionByHash",
        "params": {"transaction_hash": "'"${TX}"'"}
}' \
"${RPC}"
