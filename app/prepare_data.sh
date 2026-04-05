#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON
PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

DOC_COUNT="${DOC_COUNT:-100}"
LOCAL_DOCS_DIR="${LOCAL_DOCS_DIR:-data/generated_docs}"
HDFS_DOCS_DIR="${HDFS_DOCS_DIR:-/data}"
HDFS_INPUT_DIR="${HDFS_INPUT_DIR:-/input/data}"

PARQUET_PATH=""
for candidate in "data/n.parquet" "n.parquet" "data/a.parquet" "a.parquet"; do
    if [[ -f "$candidate" ]]; then
        PARQUET_PATH="$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"
        break
    fi
done

if [[ -z "$PARQUET_PATH" ]]; then
    echo "Parquet file not found. Expected one of: data/n.parquet, n.parquet, data/a.parquet, a.parquet"
    exit 1
fi

echo "Preparing ${DOC_COUNT} documents from ${PARQUET_PATH}"
spark-submit prepare_data.py \
    --mode generate-docs \
    --parquet "$PARQUET_PATH" \
    --output-dir "$LOCAL_DOCS_DIR" \
    --count "$DOC_COUNT"

echo "Uploading prepared documents to HDFS ${HDFS_DOCS_DIR}"
hdfs dfs -rm -r -f "$HDFS_DOCS_DIR" >/dev/null 2>&1 || true
hdfs dfs -mkdir -p "$HDFS_DOCS_DIR"
hdfs dfs -put -f "${LOCAL_DOCS_DIR}"/*.txt "$HDFS_DOCS_DIR"/

echo "Building single-partition input dataset in ${HDFS_INPUT_DIR}"
hdfs dfs -rm -r -f "$HDFS_INPUT_DIR" >/dev/null 2>&1 || true
spark-submit prepare_data.py \
    --mode build-input \
    --source-path "$HDFS_DOCS_DIR" \
    --output-path "$HDFS_INPUT_DIR"

echo "HDFS documents:"
hdfs dfs -ls "$HDFS_DOCS_DIR"

echo "Sample prepared input rows:"
hdfs dfs -cat "${HDFS_INPUT_DIR}"/part-* | head -n 3 || true

echo "Data preparation completed."
