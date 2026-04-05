#!/bin/bash

set -euo pipefail

APP_NAME_FILTER="${1:-bm25-search}"

if ! command -v yarn >/dev/null 2>&1; then
    exit 0
fi

APP_IDS=$(yarn application -list 2>/dev/null | awk -v app_name="$APP_NAME_FILTER" 'index($2, app_name) == 1 && ($6 == "ACCEPTED" || $6 == "RUNNING") {print $1}')

if [[ -z "${APP_IDS}" ]]; then
    exit 0
fi

while IFS= read -r app_id; do
    if [[ -n "${app_id}" ]]; then
        echo "Killing stale YARN application ${app_id} (${APP_NAME_FILTER})"
        yarn application -kill "${app_id}" >/dev/null 2>&1 || true
    fi
done <<< "${APP_IDS}"
