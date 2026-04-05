#!/bin/bash

set -euo pipefail

cd /app

AUTO_RUN_WORKFLOW="${AUTO_RUN_WORKFLOW:-0}"
RUN_SAMPLE_QUERIES="${RUN_SAMPLE_QUERIES:-0}"
INDEX_INPUT_PATH="${INDEX_INPUT_PATH:-/input/data}"
REQUIREMENTS_STAMP=".venv/.requirements.sha256"

service ssh restart
bash start-services.sh

if [[ ! -d .venv ]]; then
    python3 -m venv .venv
fi

source .venv/bin/activate

if command -v sha256sum >/dev/null 2>&1; then
    REQUIREMENTS_HASH=$(sha256sum requirements.txt | awk '{print $1}')
else
    REQUIREMENTS_HASH=$(python3 - <<'PY'
import hashlib
with open("requirements.txt", "rb") as fh:
    print(hashlib.sha256(fh.read()).hexdigest())
PY
)
fi

if [[ ! -f "${REQUIREMENTS_STAMP}" ]] || [[ "$(cat "${REQUIREMENTS_STAMP}")" != "${REQUIREMENTS_HASH}" ]]; then
    pip install -r requirements.txt
    printf '%s\n' "${REQUIREMENTS_HASH}" > "${REQUIREMENTS_STAMP}"
else
    echo "Python requirements are already installed."
fi

export DOC_COUNT="${DOC_COUNT:-100}"

if [[ "${AUTO_RUN_WORKFLOW}" == "1" ]]; then
    bash prepare_data.sh
    bash index.sh "${INDEX_INPUT_PATH}"

    if [[ "${RUN_SAMPLE_QUERIES}" == "1" ]]; then
        echo "Sample query 1"
        bash search.sh "history of science"

        echo "Sample query 2"
        bash search.sh "christmas song"
    fi
else
    echo "Services are ready. Automatic workflow is disabled."
    echo "Run manually inside cluster-master when needed:"
    echo "  bash /app/prepare_data.sh"
    echo "  bash /app/index.sh /data"
    echo "  bash /app/search.sh \"history of science\""
fi

tail -f /dev/null
