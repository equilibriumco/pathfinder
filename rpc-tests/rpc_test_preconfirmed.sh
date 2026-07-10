#! /usr/bin/env bash
set -e;
set -o pipefail;

# Smoke test for every JSON-RPC method that accepts the `pre_confirmed` block id.
# Calls are ordered as the methods are registered in crates/rpc/src/v10.rs.
# Subscriptions, by-hash methods and methods without a block id are omitted, as
# is starknet_getStorageProof (it returns PROOF_MISSING for pre_confirmed).
#
# Methods that need an address/class hash/request body use placeholders (0x1),
# so they may reply with a "not found" or execution error. That still exercises
# the pre_confirmed path; swap in real values from your node for a full check.

RPC="http://127.0.0.1:9546/rpc/v0_10"

function rpc_call() {
     printf "Request:\n${1}\nReply:\n"
     curl -s -X POST \
          -H 'Content-Type: application/json' \
          -d "${1}" \
          ${2}
     printf "\n\n"
}

# Connectivity check.
rpc_call \
'{"jsonrpc":"2.0","id":"0","method":"pathfinder_version"}' \
"http://127.0.0.1:9546/rpc/pathfinder/v0_1"

rpc_call \
'{
        "id": 1,
        "jsonrpc": "2.0",
        "method": "starknet_call",
        "params": {
                "request": {
                        "contract_address": "0x1",
                        "entry_point_selector": "0x0",
                        "calldata": []
                },
                "block_id": "pre_confirmed"
        }
}' \
"${RPC}"

rpc_call \
'{
        "id": 2,
        "jsonrpc": "2.0",
        "method": "starknet_estimateFee",
        "params": {
                "request": [],
                "simulation_flags": [],
                "block_id": "pre_confirmed"
        }
}' \
"${RPC}"

rpc_call \
'{
        "id": 3,
        "jsonrpc": "2.0",
        "method": "starknet_estimateMessageFee",
        "params": {
                "message": {
                        "from_address": "0x0000000000000000000000000000000000000000",
                        "to_address": "0x1",
                        "entry_point_selector": "0x0",
                        "payload": []
                },
                "block_id": "pre_confirmed"
        }
}' \
"${RPC}"

rpc_call \
'{
        "id": 4,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockTransactionCount",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 5,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockWithTxHashes",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 6,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockWithTxs",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 7,
        "jsonrpc": "2.0",
        "method": "starknet_getClass",
        "params": {"block_id": "pre_confirmed", "class_hash": "0x1"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 8,
        "jsonrpc": "2.0",
        "method": "starknet_getClassAt",
        "params": {"block_id": "pre_confirmed", "contract_address": "0x1"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 9,
        "jsonrpc": "2.0",
        "method": "starknet_getClassHashAt",
        "params": {"block_id": "pre_confirmed", "contract_address": "0x1"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 10,
        "jsonrpc": "2.0",
        "method": "starknet_getEvents",
        "params": {
                "filter": {
                        "from_block": "pre_confirmed",
                        "to_block": "pre_confirmed",
                        "chunk_size": 10
                }
        }
}' \
"${RPC}"

rpc_call \
'{
        "id": 11,
        "jsonrpc": "2.0",
        "method": "starknet_getNonce",
        "params": {"block_id": "pre_confirmed", "contract_address": "0x1"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 12,
        "jsonrpc": "2.0",
        "method": "starknet_getStateUpdate",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 13,
        "jsonrpc": "2.0",
        "method": "starknet_getStorageAt",
        "params": {"contract_address": "0x1", "key": "0x0", "block_id": "pre_confirmed"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 14,
        "jsonrpc": "2.0",
        "method": "starknet_getTransactionByBlockIdAndIndex",
        "params": {"block_id": "pre_confirmed", "index": 0}
}' \
"${RPC}"

rpc_call \
'{
        "id": 15,
        "jsonrpc": "2.0",
        "method": "starknet_getBlockWithReceipts",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"

rpc_call \
'{
        "id": 16,
        "jsonrpc": "2.0",
        "method": "starknet_simulateTransactions",
        "params": {
                "block_id": "pre_confirmed",
                "transactions": [],
                "simulation_flags": []
        }
}' \
"${RPC}"

rpc_call \
'{
        "id": 17,
        "jsonrpc": "2.0",
        "method": "starknet_traceBlockTransactions",
        "params": {"block_id": "pre_confirmed"}
}' \
"${RPC}"
