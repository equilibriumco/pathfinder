#! /usr/bin/env bash
# Diagnostic: is the feeder gateway's get_transaction_trace endpoint deprecated
# for the whole endpoint, only for recent (>= 0.13.2) transactions, or only for
# pre_confirmed ones?
#
# The starknet v0.13.2 release notes already answer the headline: feeder gateway
# tracing support was stopped as of 0.13.2, and nodes are expected to trace
# locally from then on.
# https://community.starknet.io/t/starknet-v0-13-2-pre-release-notes/114223
# This script confirms it empirically and answers the open question: whether the
# feeder still traces OLD (pre-0.13.2) transactions or dropped them too.
#
# trace_transaction's gateway fallback proxies to
#   {feeder}/get_transaction_trace?transactionHash=<hash>
# so we hit that endpoint directly for three transaction classes and classify
# each reply:
#   1. a pre_confirmed tx        (current, >= 0.13.2, not yet committed)
#   2. a recently committed tx   (current, >= 0.13.2)
#   3. an old committed tx       (pre-0.13.2, from OLD_BLOCK)
#
# Transaction hashes are pulled from pathfinder; the trace calls go straight to
# the feeder. No `set -e`: we want all three cases to run and be reported even if
# some fail.

KEY="yTyfQriq7z95qeAKO50Ik4rJBvkdtsOG1L"
RPC="http://127.0.0.1:9546/rpc/v0_10"
FEEDER="https://feeder.alpha-sepolia.starknet.io/feeder_gateway"

# A committed block before 0.13.2. Run find_last_0_13_1_block.sh to get the exact
# last-0.13.1 block for the boundary test; any pre-0.13.2 block works here.
OLD_BLOCK=86310

rpc() {
     curl -s -X POST -H 'Content-Type: application/json' -d "$1" "${RPC}"
}

feeder_trace() {
     curl -s -H "X-Throttling-Bypass: ${KEY}" \
          "${FEEDER}/get_transaction_trace?transactionHash=$1"
}

classify() {
     if [ -z "$1" ]; then
          echo "NO RESPONSE"
     elif echo "$1" | grep -qi 'deprecated'; then
          echo "DEPRECATED"
     elif echo "$1" | grep -qi 'not found\|no trace\|unknown\|invalid'; then
          echo "NOT FOUND / ERROR"
     else
          echo "SERVED (trace returned)"
     fi
}

# 1) pre_confirmed tx (last one in the pre_confirmed block).
PC_TX=$(rpc '{"id":1,"jsonrpc":"2.0","method":"starknet_getBlockWithTxHashes","params":{"block_id":"pre_confirmed"}}' \
     | jq -r '.result.transactions[-1] // empty')

# 2) recently committed tx (last one in the latest committed block).
LATEST=$(rpc '{"id":1,"jsonrpc":"2.0","method":"starknet_getBlockWithTxHashes","params":{"block_id":"latest"}}')
LATEST_TX=$(echo "${LATEST}" | jq -r '.result.transactions[-1] // empty')
LATEST_VER=$(echo "${LATEST}" | jq -r '.result.starknet_version // "?"')

# 3) old committed tx (first one in OLD_BLOCK).
OLDBLK=$(rpc '{"id":1,"jsonrpc":"2.0","method":"starknet_getBlockWithTxHashes","params":{"block_id":{"block_number":'"${OLD_BLOCK}"'}}}')
OLD_TX=$(echo "${OLDBLK}" | jq -r '.result.transactions[0] // empty')
OLD_VER=$(echo "${OLDBLK}" | jq -r '.result.starknet_version // "?"')

echo "### 1) pre_confirmed tx: ${PC_TX:-<none>}"
A=$(feeder_trace "${PC_TX}"); echo "${A}"; echo ">>> $(classify "${A}")"; echo

echo "### 2) recently committed tx (latest, version ${LATEST_VER}): ${LATEST_TX:-<none>}"
B=$(feeder_trace "${LATEST_TX}"); echo "${B}"; echo ">>> $(classify "${B}")"; echo

echo "### 3) old committed tx (block ${OLD_BLOCK}, version ${OLD_VER}): ${OLD_TX:-<none>}"
C=$(feeder_trace "${OLD_TX}"); echo "${C}"; echo ">>> $(classify "${C}")"; echo

echo "=== verdict ==="
echo "pre_confirmed         : $(classify "${A}")"
echo "committed (modern, ${LATEST_VER}) : $(classify "${B}")"
echo "committed (old,    ${OLD_VER}) : $(classify "${C}")"
echo
echo "Reading it:"
echo "  all DEPRECATED                      -> whole endpoint is gone"
echo "  1 & 2 DEPRECATED, 3 SERVED          -> version-gated (>= 0.13.2), not state"
echo "  only 1 DEPRECATED, 2 & 3 SERVED     -> pre_confirmed-specific"
