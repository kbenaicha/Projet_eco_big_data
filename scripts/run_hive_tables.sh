#!/usr/bin/env bash

set -euo pipefail
trap 'echo "Echec run_hive_tables.sh ligne ${LINENO}: ${BASH_COMMAND}" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [ -n "${GROUP:-}" ]; then
  DEFAULT_HDFS_BASE_DIR="/education/${GROUP}/${USER}/project"
else
  DEFAULT_HDFS_BASE_DIR="/user/${USER}/project"
fi

HDFS_BASE_DIR="${HDFS_BASE_DIR:-${DEFAULT_HDFS_BASE_DIR}}"
HIVE_CMD="${HIVE_CMD:-beeline}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-}"
HIVE_USER_NAME="${HIVE_USER_NAME:-${USER:-user}}"
HIVE_GROUP_NAME="${HIVE_GROUP_NAME:-${GROUP:-default}}"
DEFAULT_HIVE_USERNAME="$(printf '%s' "${HIVE_USER_NAME}_transport" | tr '.-' '__')"
HIVE_USERNAME="${HIVE_USERNAME:-${DEFAULT_HIVE_USERNAME}}"

resolve_hive_cmd() {
  local preferred="$1"

  if [ "${preferred}" = "beeline" ] && [ -x "/usr/hdp/current/hive-client/bin/beeline" ]; then
    printf '%s\n' "/usr/hdp/current/hive-client/bin/beeline"
    return 0
  fi

  if [ "${preferred}" = "beeline" ] && [ -x "/usr/hdp/3.1.0.0-78/hive/bin/beeline" ]; then
    printf '%s\n' "/usr/hdp/3.1.0.0-78/hive/bin/beeline"
    return 0
  fi

  if [ "${preferred}" = "beeline" ] && [ -x "/usr/bin/beeline" ]; then
    printf '%s\n' "/usr/bin/beeline"
    return 0
  fi

  if [ "${preferred}" = "hive" ] && [ -x "/usr/hdp/current/hive-client/bin/hive" ]; then
    printf '%s\n' "/usr/hdp/current/hive-client/bin/hive"
    return 0
  fi

  if [ "${preferred}" = "hive" ] && [ -x "/usr/hdp/3.1.0.0-78/hive/bin/hive" ]; then
    printf '%s\n' "/usr/hdp/3.1.0.0-78/hive/bin/hive"
    return 0
  fi

  if [ "${preferred}" = "hive" ] && [ -x "/usr/bin/hive" ]; then
    printf '%s\n' "/usr/bin/hive"
    return 0
  fi

  if command -v "${preferred}" >/dev/null 2>&1; then
    command -v "${preferred}"
    return 0
  fi

  return 1
}

echo "Creation des tables Hive sur ${HDFS_BASE_DIR}"
if [ "${HIVE_CMD}" = "beeline" ]; then
  if ! HIVE_CMD_BIN="$(resolve_hive_cmd beeline)"; then
    echo "La commande beeline est introuvable." >&2
    exit 1
  fi
  if [ -n "${HIVE_JDBC_URL}" ]; then
    "${HIVE_CMD_BIN}" \
      -u "${HIVE_JDBC_URL}" \
      --hivevar user="${HIVE_USER_NAME}" \
      --hivevar group="${HIVE_GROUP_NAME}" \
      --hivevar hiveUsername="${HIVE_USERNAME}" \
      --hivevar hdfs_base="${HDFS_BASE_DIR}" \
      -f hive/create_tables.hql
  else
    "${HIVE_CMD_BIN}" \
      --hivevar user="${HIVE_USER_NAME}" \
      --hivevar group="${HIVE_GROUP_NAME}" \
      --hivevar hiveUsername="${HIVE_USERNAME}" \
      --hivevar hdfs_base="${HDFS_BASE_DIR}" \
      -f hive/create_tables.hql
  fi
elif HIVE_CMD_BIN="$(resolve_hive_cmd "${HIVE_CMD}")"; then
  "${HIVE_CMD_BIN}" \
    --hivevar user="${HIVE_USER_NAME}" \
    --hivevar group="${HIVE_GROUP_NAME}" \
    --hivevar hiveUsername="${HIVE_USERNAME}" \
    --hivevar hdfs_base="${HDFS_BASE_DIR}" \
    -f hive/create_tables.hql
elif HIVE_CMD_BIN="$(resolve_hive_cmd beeline)"; then
  if [ -n "${HIVE_JDBC_URL}" ]; then
    "${HIVE_CMD_BIN}" \
      -u "${HIVE_JDBC_URL}" \
      --hivevar user="${HIVE_USER_NAME}" \
      --hivevar group="${HIVE_GROUP_NAME}" \
      --hivevar hiveUsername="${HIVE_USERNAME}" \
      --hivevar hdfs_base="${HDFS_BASE_DIR}" \
      -f hive/create_tables.hql
  else
    "${HIVE_CMD_BIN}" \
      --hivevar user="${HIVE_USER_NAME}" \
      --hivevar group="${HIVE_GROUP_NAME}" \
      --hivevar hiveUsername="${HIVE_USERNAME}" \
      --hivevar hdfs_base="${HDFS_BASE_DIR}" \
      -f hive/create_tables.hql
  fi
else
  echo "Ni ${HIVE_CMD} ni beeline ne sont disponibles." >&2
  exit 1
fi
