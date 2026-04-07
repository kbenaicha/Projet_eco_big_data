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

USER_NAME="${USER:-$(id -un 2>/dev/null || printf 'user')}"
GROUP_NAME="${GROUP:-$(id -gn 2>/dev/null || printf '')}"
export USER="${USER_NAME}"
if [ -n "${GROUP_NAME}" ]; then
  export GROUP="${GROUP_NAME}"
  DEFAULT_HDFS_BASE_DIR="/education/${GROUP_NAME}/${USER_NAME}/project"
else
  DEFAULT_HDFS_BASE_DIR="/user/${USER_NAME}/project"
fi

HDFS_BASE_DIR="${HDFS_BASE_DIR:-${DEFAULT_HDFS_BASE_DIR}}"
SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD:-spark-submit}"
PYSPARK_PYTHON_BIN="${PYSPARK_PYTHON:-python3}"
PYSPARK_DRIVER_PYTHON_BIN="${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON_BIN}}"
SPARK_SUBMIT_MASTER="${SPARK_SUBMIT_MASTER:-yarn}"
SPARK_SUBMIT_DEPLOY_MODE="${SPARK_SUBMIT_DEPLOY_MODE:-client}"

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

PYSPARK_PYTHON_RESOLVED=""
if ! PYSPARK_PYTHON_RESOLVED="$(resolve_command "${PYSPARK_PYTHON_BIN}" \
  /usr/bin/python3 /bin/python3 /usr/bin/python /bin/python)"; then
  echo "Interpreteur Python non resolu (${PYSPARK_PYTHON_BIN}), utilisation du Python par defaut de Spark."
  PYSPARK_PYTHON_RESOLVED=""
fi

PYSPARK_DRIVER_PYTHON_RESOLVED="${PYSPARK_PYTHON_RESOLVED}"
if ! PYSPARK_DRIVER_PYTHON_RESOLVED="$(resolve_command "${PYSPARK_DRIVER_PYTHON_BIN}" \
  /usr/bin/python3 /bin/python3 /usr/bin/python /bin/python)"; then
  if [ -n "${PYSPARK_PYTHON_RESOLVED}" ]; then
    PYSPARK_DRIVER_PYTHON_RESOLVED="${PYSPARK_PYTHON_RESOLVED}"
  else
    echo "Interpreteur Python driver non resolu (${PYSPARK_DRIVER_PYTHON_BIN}), utilisation du Python par defaut de Spark."
    PYSPARK_DRIVER_PYTHON_RESOLVED=""
  fi
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
echo "Spark master=${SPARK_SUBMIT_MASTER} deploy_mode=${SPARK_SUBMIT_DEPLOY_MODE}"
echo "Python Spark driver=${PYSPARK_DRIVER_PYTHON_RESOLVED:-cluster-default} executors=${PYSPARK_PYTHON_RESOLVED:-cluster-default}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
echo "YARN_CONF_DIR=${YARN_CONF_DIR}"
echo "SPARK_CONF_DIR=${SPARK_CONF_DIR}"
echo "SPARK_HOME=${SPARK_HOME}"

ENV_VARS=(
  "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
  "YARN_CONF_DIR=${YARN_CONF_DIR}"
  "SPARK_CONF_DIR=${SPARK_CONF_DIR}"
  "SPARK_HOME=${SPARK_HOME}"
  "STORAGE_MODE=hdfs"
  "HDFS_BASE_DIR=${HDFS_BASE_DIR}"
)

if [ -n "${PYSPARK_PYTHON_RESOLVED}" ]; then
  ENV_VARS+=("PYSPARK_PYTHON=${PYSPARK_PYTHON_RESOLVED}")
fi

if [ -n "${PYSPARK_DRIVER_PYTHON_RESOLVED}" ]; then
  ENV_VARS+=("PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON_RESOLVED}")
fi

SPARK_ARGS=(
  "--master" "${SPARK_SUBMIT_MASTER}"
  "--deploy-mode" "${SPARK_SUBMIT_DEPLOY_MODE}"
  "--conf" "spark.yarn.appMasterEnv.STORAGE_MODE=hdfs"
  "--conf" "spark.executorEnv.STORAGE_MODE=hdfs"
  "--conf" "spark.yarn.appMasterEnv.HDFS_BASE_DIR=${HDFS_BASE_DIR}"
  "--conf" "spark.executorEnv.HDFS_BASE_DIR=${HDFS_BASE_DIR}"
)

if [ -n "${GROUP_NAME}" ]; then
  SPARK_ARGS+=("--conf" "spark.yarn.appMasterEnv.GROUP=${GROUP_NAME}")
  SPARK_ARGS+=("--conf" "spark.executorEnv.GROUP=${GROUP_NAME}")
fi

if [ -n "${USER_NAME}" ]; then
  SPARK_ARGS+=("--conf" "spark.yarn.appMasterEnv.USER=${USER_NAME}")
  SPARK_ARGS+=("--conf" "spark.executorEnv.USER=${USER_NAME}")
fi

if [ -n "${PYSPARK_PYTHON_RESOLVED}" ]; then
  SPARK_ARGS+=("--conf" "spark.pyspark.python=${PYSPARK_PYTHON_RESOLVED}")
  SPARK_ARGS+=("--conf" "spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON_RESOLVED}")
  SPARK_ARGS+=("--conf" "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON_RESOLVED}")
fi

if [ -n "${PYSPARK_DRIVER_PYTHON_RESOLVED}" ]; then
  SPARK_ARGS+=("--conf" "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON_RESOLVED}")
  SPARK_ARGS+=("--conf" "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON_RESOLVED}")
fi

env "${ENV_VARS[@]}" "${SPARK_SUBMIT_BIN}" "${SPARK_ARGS[@]}" "${STEP_SCRIPT}"
