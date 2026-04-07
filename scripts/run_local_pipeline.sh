#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

DEFAULT_PYTHON="python"

# Cas Linux / Mac
if [ -x ".venv/bin/python" ]; then
  DEFAULT_PYTHON=".venv/bin/python"
fi

# Cas Windows
if [ -x ".venv/Scripts/python.exe" ]; then
  DEFAULT_PYTHON=".venv/Scripts/python.exe"
fi

PYTHON_CMD="${PYTHON_CMD:-${DEFAULT_PYTHON}}"
PYSPARK_PYTHON_BIN="${PYSPARK_PYTHON:-${PYTHON_CMD}}"
PYSPARK_DRIVER_PYTHON_BIN="${PYSPARK_DRIVER_PYTHON:-${PYTHON_CMD}}"

if [ -z "${JAVA_HOME:-}" ] && command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA_17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  if [ -n "${JAVA_17_HOME}" ]; then
    export JAVA_HOME="${JAVA_17_HOME}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
  fi
fi

echo "Execution locale de la pipeline hybride"
if [ "${COLLECT_REALTIME_FIRST:-false}" = "true" ]; then
  echo "Collecte des snapshots temps reel"
  "${PYTHON_CMD}" src/collect_realtime.py
fi

PYSPARK_PYTHON="${PYSPARK_PYTHON_BIN}" \
PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON_BIN}" \
"${PYTHON_CMD}" src/prepare_gtfs.py

PYSPARK_PYTHON="${PYSPARK_PYTHON_BIN}" \
PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON_BIN}" \
"${PYTHON_CMD}" src/prepare_realtime.py

PYSPARK_PYTHON="${PYSPARK_PYTHON_BIN}" \
PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON_BIN}" \
"${PYTHON_CMD}" src/build_hybrid_table.py

PYSPARK_PYTHON="${PYSPARK_PYTHON_BIN}" \
PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON_BIN}" \
"${PYTHON_CMD}" src/build_analytics.py

echo "Pipeline locale terminee."
