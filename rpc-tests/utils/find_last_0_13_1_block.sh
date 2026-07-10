#! /usr/bin/env bash
# Find the last block with starknet version < 0.13.2 (the last 0.13.1.x block),
# i.e. the last block before feeder-gateway tracing was dropped in 0.13.2.
#
# Binary search over the committed chain, relying on the starknet version being
# monotonic non-decreasing in block height. Prints the boundary: the last
# < 0.13.2 block and the first >= 0.13.2 block, with their versions.

RPC="http://127.0.0.1:9546/rpc/v0_10"

rpc() {
     curl -s -X POST -H 'Content-Type: application/json' -d "$1" "${RPC}"
}

block_version() {
     rpc '{"id":1,"jsonrpc":"2.0","method":"starknet_getBlockWithTxHashes","params":{"block_id":{"block_number":'"$1"'}}}' \
          | jq -r '.result.starknet_version // empty'
}

# True (0) if version $1 is strictly less than $2, compared component by
# component numerically (so 0.13.1.1 < 0.13.2 < 0.14.0). Missing components count
# as 0; an empty version counts as very old.
ver_lt() {
     [ -z "$1" ] && return 0
     local IFS=. a b n i x y
     a=($1); b=($2)
     n=${#a[@]}; [ ${#b[@]} -gt "${n}" ] && n=${#b[@]}
     for ((i = 0; i < n; i++)); do
          x=${a[i]:-0}; y=${b[i]:-0}
          if ((10#$x < 10#$y)); then return 0; fi
          if ((10#$x > 10#$y)); then return 1; fi
     done
     return 1
}

HEAD=$(rpc '{"id":1,"jsonrpc":"2.0","method":"starknet_blockNumber"}' | jq -r '.result')
if ! [[ "${HEAD}" =~ ^[0-9]+$ ]]; then
     echo "Could not fetch committed head (got: '${HEAD}')" >&2
     exit 1
fi

# Largest block whose version is still < 0.13.2.
lo=0; hi=${HEAD}
while [ "${lo}" -lt "${hi}" ]; do
     mid=$(( (lo + hi + 1) / 2 ))
     if ver_lt "$(block_version "${mid}")" "0.13.2"; then lo=${mid}; else hi=$((mid - 1)); fi
done

LAST=${lo}
LAST_VER=$(block_version "${LAST}")
NEXT=$((LAST + 1))
NEXT_VER=$(block_version "${NEXT}")

echo "last  < 0.13.2 : block ${LAST} (version ${LAST_VER})"
echo "first >= 0.13.2: block ${NEXT} (version ${NEXT_VER})"
echo
echo "${LAST}"
