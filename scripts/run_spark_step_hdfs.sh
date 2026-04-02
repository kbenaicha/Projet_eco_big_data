#!/usr/bin/env bash

set -euo pipefail
trap 'echo "Echec run_spark_step_hdfs.sh ligne ${LINENO}: ${BASH_COMMAND}" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

STEP_SCRIPT="${1:-}"
if [ -z "${STEP_SCRIPT}" ]; then
  echo "Usage: bash scripts/run_spark_step_hdfs.sh <src/script.py>" >&2
  exit 1
fi

if [ ! -f "${STEP_SCRIPT}" ]; then
  echo "Script Spark introuvable: ${STEP_SCRIPT}" >&2
  exit 1
fi

if [ -n "${GROUP:-}" ]; then
  DEFAULT_HDFS_BASE_DIR="/education/${GROUP}/${USER}/project"
else
  DEFAULT_HDFS_BASE_DIR="/user/${USER}/project"
fi

HDFS_BASE_DIR="${HDFS_BASE_DIR:-${DEFAULT_HDFS_BASE_DIR}}"
SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD:-spark-submit}"
PYSPARK_PYTHON_BIN="${PYSPARK_PYTHON:-python3}"
PYSPARK_DRIVER_PYTHON_BIN="${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON_BIN}}"

resolve_command() {
  local preferred="$1"
  shift

  if [ -n "${preferred}" ] && [ -x "${preferred}" ]; then
    printf '%s\n' "${preferred}"
    return 0
  fi

  while [ "$#" -gt 0 ]; do
    if [ -x "$1" ]; then
      printf '%s\n' "$1"
      return 0
    fi
    shift
  done

  if command -v "${preferred}" >/dev/null 2>&1; then
    command -v "${preferred}"
    return 0
  fi

  return 1
}

if ! PYSPARK_PYTHON_RESOLVED="$(resolve_command "${PYSPARK_PYTHON_BIN}" /usr/bin/python3 /bin/python3)"; then
  echo "Interpreteur Python introuvable: ${PYSPARK_PYTHON_BIN}" >&2
  exit 1
fi

if ! PYSPARK_DRIVER_PYTHON_RESOLVED="$(resolve_command "${PYSPARK_DRIVER_PYTHON_BIN}" /usr/bin/python3 /bin/python3)"; then
  echo "Interpreteur Python driver introuvable: ${PYSPARK_DRIVER_PYTHON_BIN}" >&2
  exit 1
fi

HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
YARN_CONF_DIR="${YARN_CONF_DIR:-${HADOOP_CONF_DIR}}"
SPARK_CONF_DIR="${SPARK_CONF_DIR:-/etc/spark2/conf}"
SPARK_HOME="${SPARK_HOME:-}"
if [ ! -d "${SPARK_CONF_DIR}" ] && [ -d "/usr/hdp/current/spark2-client/conf" ]; then
  SPARK_CONF_DIR="/usr/hdp/current/spark2-client/conf"
fi

if [ -z "${SPARK_HOME}" ]; then
  for candidate in \
    /usr/hdp/3.1.0.0-78/spark2 \
    /usr/hdp/current/spark2-client \
    /usr/hdp/3.1.0.0-78/spark2-client \
    /usr/lib/spark; do
    if [ -d "${candidate}" ]; then
      SPARK_HOME="${candidate}"
      break
    fi
  done
fi

if [ -n "${SPARK_HOME}" ] && [ -x "${SPARK_HOME}/bin/spark-submit" ] && [ "${SPARK_SUBMIT_CMD}" = "spark-submit" ]; then
  SPARK_SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit"
fi

if ! SPARK_SUBMIT_BIN="$(resolve_command "${SPARK_SUBMIT_CMD}" \
  /usr/hdp/current/spark2-client/bin/spark-submit \
  /usr/hdp/3.1.0.0-78/spark2/bin/spark-submit \
  /usr/hdp/3.1.0.0-78/spark2-client/bin/spark-submit \
  /usr/bin/spark2-submit \
  /usr/lib/spark/bin/spark-submit)"; then
  echo "Commande spark-submit introuvable: ${SPARK_SUBMIT_CMD}" >&2
  exit 1
fi

echo "Execution Spark step=${STEP_SCRIPT} base=${HDFS_BASE_DIR}"
echo "Commande spark-submit=${SPARK_SUBMIT_BIN}"
echo "Python Spark driver=${PYSPARK_DRIVER_PYTHON_RESOLVED} executors=${PYSPARK_PYTHON_RESOLVED}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
echo "YARN_CONF_DIR=${YARN_CONF_DIR}"
echo "SPARK_CONF_DIR=${SPARK_CONF_DIR}"
echo "SPARK_HOME=${SPARK_HOME}"

PYSPARK_PYTHON="${PYSPARK_PYTHON_RESOLVED}" \
PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON_RESOLVED}" \
HADOOP_CONF_DIR="${HADOOP_CONF_DIR}" \
YARN_CONF_DIR="${YARN_CONF_DIR}" \
SPARK_CONF_DIR="${SPARK_CONF_DIR}" \
SPARK_HOME="${SPARK_HOME}" \
STORAGE_MODE=hdfs HDFS_BASE_DIR="${HDFS_BASE_DIR}" "${SPARK_SUBMIT_BIN}" "${STEP_SCRIPT}"
