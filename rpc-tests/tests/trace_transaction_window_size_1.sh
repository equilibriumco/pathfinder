#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_traceTransaction for a tx in a depth-1 pre_confirmed window.
#
# When the pre_confirmed tip is the immediate child of the committed head
# (tip == committed + 1), a tx in the tip is executed LOCALLY: its block is
# committed + 1, whose parent state is the committed head in the DB. This is the
# only case trace_transaction runs locally; anything deeper falls back to the
# gateway.
#
# The window depth isn't controllable, so we poll until the tip is committed + 1
# and has at least one tx, then trace that tx. Best-effort: state may shift
# between the check and the trace call.

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

committed_head() {
     curl -s -X POST -H 'Content-Type: application/json' \
          -d '{"id": 1, "jsonrpc": "2.0", "method": "starknet_blockNumber"}' \
          "${RPC}" | jq -r '.result'
}

pre_confirmed_block() {
     curl -s -X POST -H 'Content-Type: application/json' \
          -d '{"id": 1, "jsonrpc": "2.0", "method": "starknet_getBlockWithTxHashes", "params": {"block_id": "pre_confirmed"}}' \
          "${RPC}"
}

# Wait for a depth-1 tip that carries at least one transaction.
TX=""
for _ in $(seq 1 60); do
     C=$(committed_head)
     PC=$(pre_confirmed_block)
     T=$(echo "${PC}" | jq -r '.result.block_number')
     LAST=$(echo "${PC}" | jq -r '.result.transactions[-1] // empty')
     if [[ "${C}" =~ ^[0-9]+$ ]] && [[ "${T}" =~ ^[0-9]+$ ]]; then
          DEPTH=$((T - C))
          if [ "${DEPTH}" -eq 1 ] && [ -n "${LAST}" ]; then TX="${LAST}"; break; fi
     fi
     sleep 0.5
done

if [ -z "${TX}" ]; then
     echo "Gave up waiting for a depth-1 tip with a transaction (last: tip ${T}, committed ${C})." >&2
     echo "Re-run when the committed head is at tip - 1 and the tip has a tx." >&2
     exit 0
fi
echo "pre_confirmed tip ${T} is committed + 1; tracing tx ${TX} locally"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_traceTransaction",
        "params": {"transaction_hash": "'"${TX}"'"}
}' \
"${RPC}"
