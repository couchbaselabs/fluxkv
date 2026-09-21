#!/usr/bin/env bash
# Run overwrite.sh once per value size at a fixed live dataset.
#   CONFIG=tuned DATA_DIR=/data/fluxkv-ow bench/overwrite/value_sweep.sh 1024 256 64
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
for VS in "$@"; do
  # smaller values mean more keys; give the load time to finish
  LOAD=${LOAD:-$(( VS >= 1024 ? 150 : 1500 ))} VS=$VS TAG=${CONFIG:-baseline}-$VS "$HERE/overwrite.sh"
done
