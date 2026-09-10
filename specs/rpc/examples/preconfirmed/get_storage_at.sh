#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getStorageAt against the pre_confirmed block.
#
# Reads a storage slot of the test contract as seen at pre_confirmed: the
# pre_confirmed overlay is consulted first, then the committed state. The slot is
# the contract's `balance` variable, so nothing has to be fetched.
#
#   contract_address = the block-poke test contract
#   key              = starknet_keccak("balance"), the `balance` storage slot
#
# An unset/mismatched slot simply reads back 0x0 (getStorageAt does not error),
# so this still exercises the pre_confirmed read path either way.

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getStorageAt",
  "params": {
    "contract_address": "0x026161f4a753e6940fc82637bacb02ea62fdff46e7197d02f4768cdc9b3b7428",
    "key": "0x206f38f7e4f15e87567361213c28f235cccdaa1d7fd34c9db1dfe9489c6a091",
    "block_id": "pre_confirmed"
  }
}'
