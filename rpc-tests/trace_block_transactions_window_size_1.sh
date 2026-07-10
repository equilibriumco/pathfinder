#! /usr/bin/env bash
set -e;
set -o pipefail;

# starknet_traceBlockTransactions against the pre_confirmed block, depth-1 case.
#
# Here the pre_confirmed tip is the immediate child of the committed head
# (tip == committed + 1). The executor's base state is the committed head, which
# is already in the DB, so no parents overlay is needed: this is plain local
# execution of the tip's transactions.
#
# The window depth isn't controllable, so we poll until the tip is committed + 1
# and then trace. Best-effort: the tip may advance between the check and the
# trace call.

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

# Wait for the tip to sit exactly one above the committed head.
DEPTH=""
for _ in $(seq 1 60); do
     C=$(committed_head)
     T=$(pre_confirmed_tip)
     if [[ "${C}" =~ ^[0-9]+$ ]] && [[ "${T}" =~ ^[0-9]+$ ]]; then
          DEPTH=$((T - C))
          if [ "${DEPTH}" -eq 1 ]; then break; fi
     fi
     sleep 0.5
done

if [ "${DEPTH}" != "1" ]; then
     echo "Gave up waiting for a depth-1 window (last: tip ${T}, committed ${C}, depth ${DEPTH})." >&2
     echo "Re-run when the committed head has caught up to tip - 1." >&2
     exit 0
fi
echo "pre_confirmed tip ${T} is committed + 1 (committed ${C}); tracing locally on the committed state"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_traceBlockTransactions",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"
