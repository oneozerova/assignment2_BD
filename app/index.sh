#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

INPUT_PATH="${1:-/input/data}"
NORMALIZED_INPUT_PATH="$INPUT_PATH"
TEMP_INPUT_PATH="/tmp/indexer_input"

./cleanup_yarn_apps.sh bm25-search
./cleanup_yarn_apps.sh search-engine-pipeline

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON
PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

if hdfs dfs -ls "${INPUT_PATH}" 2>/dev/null | awk '{print $8}' | grep -q '\.txt$'; then
    echo "Detected raw document folder at ${INPUT_PATH}. Building temporary single-partition input dataset."
    hdfs dfs -rm -r -f "${TEMP_INPUT_PATH}" >/dev/null 2>&1 || true
    spark-submit prepare_data.py \
        --mode build-input \
        --source-path "${INPUT_PATH}" \
        --output-path "${TEMP_INPUT_PATH}"
    NORMALIZED_INPUT_PATH="${TEMP_INPUT_PATH}"
fi

echo "Indexing HDFS input path: ${NORMALIZED_INPUT_PATH}"
bash create_index.sh "$NORMALIZED_INPUT_PATH"
bash store_index.sh
