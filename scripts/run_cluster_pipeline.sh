#!/usr/bin/env bash

set -euo pipefail
trap 'echo "Echec run_cluster_pipeline.sh ligne ${LINENO}: ${BASH_COMMAND}" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

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
UPLOAD_RAW_FIRST="${UPLOAD_RAW_FIRST:-true}"
PUBLISH_HIVE="${PUBLISH_HIVE:-false}"
CLUSTER_PATH_DEFAULT="/usr/hdp/current/spark2-client/bin:/usr/hdp/3.1.0.0-78/spark2/bin:/usr/hdp/current/hadoop-client/bin:/usr/hdp/3.1.0.0-78/hadoop/bin:/usr/hdp/current/hive-client/bin:/usr/hdp/3.1.0.0-78/hive/bin:/usr/bin:/bin"
export PATH="${CLUSTER_PATH_DEFAULT}${PATH:+:${PATH}}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-${HADOOP_CONF_DIR}}"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/etc/spark2/conf}"
export SPARK_HOME="${SPARK_HOME:-/usr/hdp/3.1.0.0-78/spark2}"
export SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD:-/usr/hdp/3.1.0.0-78/spark2/bin/spark-submit}"
export SPARK_SUBMIT_MASTER="${SPARK_SUBMIT_MASTER:-yarn}"
export SPARK_SUBMIT_DEPLOY_MODE="${SPARK_SUBMIT_DEPLOY_MODE:-client}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON}}"
export HIVE_CMD="${HIVE_CMD:-/usr/hdp/3.1.0.0-78/hive/bin/beeline}"

echo "Execution pipeline cluster"
echo "USER=${USER_NAME}"
echo "GROUP=${GROUP_NAME:-<none>}"
echo "HDFS_BASE_DIR=${HDFS_BASE_DIR}"
echo "UPLOAD_RAW_FIRST=${UPLOAD_RAW_FIRST}"
echo "PUBLISH_HIVE=${PUBLISH_HIVE}"
echo "PATH=${PATH}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
echo "YARN_CONF_DIR=${YARN_CONF_DIR}"
echo "SPARK_CONF_DIR=${SPARK_CONF_DIR}"
echo "SPARK_HOME=${SPARK_HOME}"
echo "SPARK_SUBMIT_CMD=${SPARK_SUBMIT_CMD}"
echo "SPARK_SUBMIT_MASTER=${SPARK_SUBMIT_MASTER}"
echo "SPARK_SUBMIT_DEPLOY_MODE=${SPARK_SUBMIT_DEPLOY_MODE}"

if [ "${UPLOAD_RAW_FIRST}" = "true" ]; then
  echo
  echo "[1/6] Upload des donnees brutes vers HDFS"
  GROUP="${GROUP:-}" USER="${USER:-}" HDFS_BASE_DIR="${HDFS_BASE_DIR}/raw" \
    bash scripts/upload_raw_to_hdfs_cluster.sh
fi

echo
echo "[2/6] Preparation GTFS"
bash scripts/run_spark_step_hdfs.sh src/prepare_gtfs.py

echo
echo "[3/6] Preparation temps reel"
bash scripts/run_spark_step_hdfs.sh src/prepare_realtime.py

echo
echo "[4/6] Construction de la table hybride"
bash scripts/run_spark_step_hdfs.sh src/build_hybrid_table.py

echo
echo "[5/6] Calcul des analytics"
bash scripts/run_spark_step_hdfs.sh src/build_analytics.py

echo
echo "[6/6] Generation du rapport lisible"
bash scripts/run_spark_step_hdfs.sh src/build_results_report.py

if [ "${PUBLISH_HIVE}" = "true" ]; then
  echo
  echo "[7/7] Publication Hive"
  bash scripts/run_hive_tables.sh
fi

echo
echo "Pipeline cluster terminee."
