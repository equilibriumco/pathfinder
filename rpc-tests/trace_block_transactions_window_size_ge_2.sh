#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_traceBlockTransactions against the pre_confirmed block, deep-window
# case.
#
# Here the pre_confirmed tip sits >= 2 blocks above the committed head, so there
# is a multi-block un-committed window below it. The executor can't read the
# tip's parent from the DB (it isn't committed), so it traces the tip on the
# committed state plus the composed parents overlay (the window fill). This is
# the path the trace_block_transactions overlay fix added.
#
# The window depth isn't controllable, so we poll until the tip leads the
# committed head by >= 2 and then trace. Best-effort: the depth may change
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

pre_confirmed_tip() {
     curl -s -X POST -H 'Content-Type: application/json' \
          -d '{"id": 1, "jsonrpc": "2.0", "method": "starknet_getBlockWithTxHashes", "params": {"block_id": "pre_confirmed"}}' \
          "${RPC}" | jq -r '.result.block_number'
}

# Wait for the tip to lead the committed head by at least two.
DEPTH=""
for _ in $(seq 1 60); do
     C=$(committed_head)
     T=$(pre_confirmed_tip)
     if [[ "${C}" =~ ^[0-9]+$ ]] && [[ "${T}" =~ ^[0-9]+$ ]]; then
          DEPTH=$((T - C))
          if [ "${DEPTH}" -ge 2 ]; then break; fi
     fi
     sleep 0.5
done

if [ -z "${DEPTH}" ] || [ "${DEPTH}" -lt 2 ]; then
     echo "Gave up waiting for a depth >= 2 window (last: tip ${T}, committed ${C}, depth ${DEPTH})." >&2
     echo "Re-run when the pre_confirmed tip leads the committed head by >= 2." >&2
     exit 0
fi
echo "pre_confirmed tip ${T} is committed + ${DEPTH} (committed ${C}); tracing on committed + parents overlay"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_traceBlockTransactions",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"
