#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_traceTransaction for a pre_confirmed transaction, plain version.
#
# Traces the last transaction in the current pre_confirmed block, regardless of
# the window depth (see the _window_size_1 / _window_size_ge_2 variants for the
# empty-overlay vs parents-overlay cases). No block_id: the tx is found in the
# pre_confirmed window and traced locally.
#
# Best-effort: by trace time the tx may already be committed.

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
        "method": "starknet_traceTransaction",
        "params": {"transaction_hash": "'"${TX}"'"}
}' \
"${RPC}"
