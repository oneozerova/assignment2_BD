#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

INPUT_PATH="${1:-/input/data}"
TMP_INDEX_ROOT="${TMP_INDEX_ROOT:-/tmp/indexer}"
INDEX_ROOT="${INDEX_ROOT:-/indexer}"

if ! hdfs dfs -test -e "$INPUT_PATH"; then
    echo "Input path ${INPUT_PATH} does not exist in HDFS."
    exit 1
fi

STREAMING_JAR=$(find "${HADOOP_HOME}/share/hadoop/tools/lib" -name "hadoop-streaming*.jar" | head -n 1)
if [[ -z "$STREAMING_JAR" ]]; then
    echo "Unable to locate hadoop-streaming jar."
    exit 1
fi

echo "Cleaning previous index data"
hdfs dfs -rm -r -f "$TMP_INDEX_ROOT" >/dev/null 2>&1 || true
hdfs dfs -rm -r -f "$INDEX_ROOT" >/dev/null 2>&1 || true
hdfs dfs -mkdir -p "$TMP_INDEX_ROOT"

echo "Running pipeline 1: documents, postings, corpus stats"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="search-engine-pipeline-1" \
    -D mapreduce.job.reduces=1 \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output "${TMP_INDEX_ROOT}/pipeline1"

echo "Running pipeline 2: vocabulary and document frequency"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="search-engine-pipeline-2" \
    -D mapreduce.job.reduces=1 \
    -files mapreduce/mapper2.py,mapreduce/reducer2.py \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input "${TMP_INDEX_ROOT}/pipeline1" \
    -output "${TMP_INDEX_ROOT}/pipeline2"

echo "Publishing index to ${INDEX_ROOT}"
hdfs dfs -mkdir -p "$INDEX_ROOT"
hdfs dfs -cp "${TMP_INDEX_ROOT}/pipeline1" "${INDEX_ROOT}/pipeline1"
hdfs dfs -cp "${TMP_INDEX_ROOT}/pipeline2" "${INDEX_ROOT}/pipeline2"

echo "Indexer outputs:"
hdfs dfs -ls "${INDEX_ROOT}/pipeline1"
hdfs dfs -ls "${INDEX_ROOT}/pipeline2"

echo "Index creation completed."
