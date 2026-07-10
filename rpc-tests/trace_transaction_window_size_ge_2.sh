#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_traceTransaction for a tx in the tip of a deep pre_confirmed window.
#
# When the pre_confirmed tip is >= 2 blocks above the committed head, a tx in the
# tip is traced locally on the committed state plus the composed diffs of the
# parents below it, covering (committed, tip - 1]. This is the deep-window overlay
# path. The gateway is not used (its trace endpoint is deprecated).
#
# The window depth isn't controllable, so we poll until the tip leads the
# committed head by >= 2 and has at least one tx, then trace that tx.
# Best-effort: state may shift between the check and the trace call.

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

# Wait for a tip that leads committed by >= 2 and carries at least one tx.
TX=""
for _ in $(seq 1 60); do
     C=$(committed_head)
     PC=$(pre_confirmed_block)
     T=$(echo "${PC}" | jq -r '.result.block_number')
     LAST=$(echo "${PC}" | jq -r '.result.transactions[-1] // empty')
     if [[ "${C}" =~ ^[0-9]+$ ]] && [[ "${T}" =~ ^[0-9]+$ ]]; then
          DEPTH=$((T - C))
          if [ "${DEPTH}" -ge 2 ] && [ -n "${LAST}" ]; then TX="${LAST}"; break; fi
     fi
     sleep 0.5
done

if [ -z "${TX}" ]; then
     echo "Gave up waiting for a depth >= 2 tip with a transaction (last: tip ${T}, committed ${C})." >&2
     echo "Re-run when the pre_confirmed tip leads the committed head by >= 2 and has a tx." >&2
     exit 0
fi
echo "pre_confirmed tip ${T} is committed + $((T - C)); tracing tx ${TX} locally on committed + parents overlay"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_traceTransaction",
        "params": {"transaction_hash": "'"${TX}"'"}
}' \
"${RPC}"
