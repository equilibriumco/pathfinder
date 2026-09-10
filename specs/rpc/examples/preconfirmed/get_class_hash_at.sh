#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getClassHashAt against the pre_confirmed block.
#
# Returns the class hash of the contract at the test contract's address, as seen
# at pre_confirmed (pending overlay first, then the DB). The contract address is
# a fixed input, so nothing has to be fetched.

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getClassHashAt",
  "params": {
    "block_id": "pre_confirmed",
    "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428"
  }
}'
