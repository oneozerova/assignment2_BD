#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

QUERY_TEXT="${*:-}"
SEARCH_TIMEOUT_SEC="${SEARCH_TIMEOUT_SEC:-30}"
SEARCH_MODE="${SEARCH_MODE:-auto}"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON
PYSPARK_DRIVER_PYTHON=$(which python)

run_yarn_search() {
    export PYSPARK_PYTHON=python3
    timeout "${SEARCH_TIMEOUT_SEC}" spark-submit \
        --master yarn \
        --deploy-mode client \
        /app/query.py \
        --query "$QUERY_TEXT"
}

run_local_search() {
    export PYSPARK_PYTHON
    PYSPARK_PYTHON=$(which python)
    spark-submit \
        --master local[*] \
        /app/query.py \
        --query "$QUERY_TEXT"
}

./cleanup_yarn_apps.sh bm25-search

if [[ "${SEARCH_MODE}" == "local" ]]; then
    run_local_search
    exit 0
fi

if [[ "${SEARCH_MODE}" == "yarn" ]]; then
    run_yarn_search
    exit 0
fi

if run_yarn_search; then
    exit 0
fi

./cleanup_yarn_apps.sh bm25-search

echo "YARN search did not complete successfully within ${SEARCH_TIMEOUT_SEC}s. Falling back to local Spark execution."
run_local_search
