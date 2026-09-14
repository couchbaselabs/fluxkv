#!/usr/bin/env bash
#
# Sweep the IO path: {libaio, io_uring} x {DirectIO, buffered}.
#
# Restarts the server for each configuration, warms it, then measures a peak
# and a moderate load point.
#
# What this measured on a 500M x 1KB dataset with a 64G quota:
#
#   config              peak      moderate
#   libaio  + directio  906K/s    548K/s @ p50 871us
#   io_uring+ directio  915K/s    535K/s @ p50 901us
#   io_uring+ buffered  908K/s    887K/s @ p50 443us
#   libaio  + buffered  920K/s    881K/s @ p50 445us
#
# Two conclusions. io_uring and libaio are indistinguishable here. Buffered IO
# looks far better at moderate load, but only because it borrows free RAM as
# page cache - see cgroup_run.sh, which takes that RAM away and collapses the
# advantage.
#
# Usage: DATA_DIR=/data/dataset KEYS=500000000 bench/io_matrix.sh

set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export DATA_DIR=${DATA_DIR:?set DATA_DIR to the dataset directory}
export KEYS=${KEYS:?set KEYS to the dataset key count}
export PORT=${PORT:-12210}
export HOST=${HOST:-127.0.0.1:${PORT}}

run_config() {
    local label=$1 async_io=$2 direct_io=$3

    echo "######## ${label} ########"
    MAGMA_ASYNC_IO="${async_io}" MAGMA_DIRECT_IO="${direct_io}" \
        "${HERE}/run_server.sh"

    LABEL="${label}" "${HERE}/measure.sh"
}

run_config "libaio+directio"   libaio 1
run_config "io_uring+directio" uring  1
run_config "io_uring+buffered" uring  0
run_config "libaio+buffered"   libaio 0
