# shellcheck shell=bash
# Shared helpers for the pre_confirmed RPC examples. Sourced, not executed.

# Override with RPC=<url> to target a different node.
RPC="${RPC:-http://127.0.0.1:9546/rpc/v0_10}"

function rpc_call_raw() {
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          "${RPC}"
}

function rpc_call() {
     printf 'Request:\n%s\nReply:\n' "${1}"
     rpc_call_raw "${1}"
     printf '\n\n'
}
