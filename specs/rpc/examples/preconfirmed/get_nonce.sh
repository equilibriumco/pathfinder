#! /usr/bin/env bash

# provides rpc_call function, custom endpoint can be set with RPC env var
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -euo pipefail

# starknet_getNonce against the pre_confirmed block.
#
# Returns the account's nonce as seen at pre_confirmed: the pre_confirmed data is
# consulted first (so in-flight txs are reflected), falling back to the committed
# nonce. The account address is a fixed input, so nothing has to be fetched.

rpc_call '{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "starknet_getNonce",
  "params": {
    "block_id": "pre_confirmed",
    "contract_address": "0x3c7e0c59fecffcdfc5c65762bf8c70533de8359497a735a35674eb357f43ff6"
  }
}'
