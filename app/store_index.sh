#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

source .venv/bin/activate

PIPELINE1_PATH="${PIPELINE1_PATH:-/indexer/pipeline1}"
PIPELINE2_PATH="${PIPELINE2_PATH:-/indexer/pipeline2}"

hdfs dfs -ls "${PIPELINE1_PATH}/part-*" >/dev/null
hdfs dfs -ls "${PIPELINE2_PATH}/part-*" >/dev/null

python app.py \
    --load-index \
    --pipeline1-path "$PIPELINE1_PATH" \
    --pipeline2-path "$PIPELINE2_PATH"
