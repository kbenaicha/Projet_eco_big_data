#!/usr/bin/env bash

set -euo pipefail
trap 'echo "Echec upload_raw_to_hdfs_cluster.sh ligne ${LINENO}: ${BASH_COMMAND}" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [ -n "${GROUP:-}" ]; then
  DEFAULT_HDFS_BASE_DIR="/education/${GROUP}/${USER}/project/raw"
else
  DEFAULT_HDFS_BASE_DIR="/user/${USER}/project/raw"
fi

HDFS_BASE_DIR="${HDFS_BASE_DIR:-${DEFAULT_HDFS_BASE_DIR}}"
LOCAL_RAW_DIR="${LOCAL_RAW_DIR:-data/raw}"
GTFS_PATTERN="${LOCAL_RAW_DIR}/gtfs/IDFM-gtfs/"*.txt
REALTIME_PATTERN="${LOCAL_RAW_DIR}/realtime/"*.json

resolve_hdfs_cmd() {
  if [ -n "${HDFS_CMD:-}" ]; then
    HDFS_CMD_BIN="${HDFS_CMD}"
    HDFS_FS_SUBCOMMAND="dfs"
    return 0
  fi

  if [ -x "/usr/hdp/current/hadoop-client/bin/hdfs" ]; then
    HDFS_CMD_BIN="/usr/hdp/current/hadoop-client/bin/hdfs"
    HDFS_FS_SUBCOMMAND="dfs"
    return 0
  fi

  if command -v hdfs >/dev/null 2>&1; then
    HDFS_CMD_BIN="$(command -v hdfs)"
    HDFS_FS_SUBCOMMAND="dfs"
    return 0
  fi

  if [ -x "/usr/hdp/current/hadoop-client/bin/hadoop" ]; then
    HDFS_CMD_BIN="/usr/hdp/current/hadoop-client/bin/hadoop"
    HDFS_FS_SUBCOMMAND="fs"
    return 0
  fi

  if command -v hadoop >/dev/null 2>&1; then
    HDFS_CMD_BIN="$(command -v hadoop)"
    HDFS_FS_SUBCOMMAND="fs"
    return 0
  fi

  return 1
}

if ! resolve_hdfs_cmd; then
  echo "Aucune commande HDFS/Hadoop n'est disponible. Definis HDFS_CMD ou lance ce script depuis un noeud Hadoop client." >&2
  exit 1
fi

if ! ls ${GTFS_PATTERN} >/dev/null 2>&1; then
  echo "Aucun fichier GTFS trouve dans ${LOCAL_RAW_DIR}/gtfs/IDFM-gtfs" >&2
  exit 1
fi

if ! ls ${REALTIME_PATTERN} >/dev/null 2>&1; then
  echo "Aucun snapshot JSON trouve dans ${LOCAL_RAW_DIR}/realtime" >&2
  exit 1
fi

echo "Chargement local -> HDFS depuis ${LOCAL_RAW_DIR}"
echo "Repertoire de travail: $(pwd)"
echo "Commande HDFS: ${HDFS_CMD_BIN} ${HDFS_FS_SUBCOMMAND}"
echo "GTFS detectes: $(find "${LOCAL_RAW_DIR}/gtfs/IDFM-gtfs" -maxdepth 1 -type f | wc -l | tr -d ' ')"
echo "Snapshots realtime detectes: $(find "${LOCAL_RAW_DIR}/realtime" -maxdepth 1 -type f -name '*.json' | wc -l | tr -d ' ')"
"${HDFS_CMD_BIN}" "${HDFS_FS_SUBCOMMAND}" -mkdir -p "${HDFS_BASE_DIR}/gtfs"
"${HDFS_CMD_BIN}" "${HDFS_FS_SUBCOMMAND}" -mkdir -p "${HDFS_BASE_DIR}/realtime"

"${HDFS_CMD_BIN}" "${HDFS_FS_SUBCOMMAND}" -put -f ${GTFS_PATTERN} "${HDFS_BASE_DIR}/gtfs/"
"${HDFS_CMD_BIN}" "${HDFS_FS_SUBCOMMAND}" -put -f ${REALTIME_PATTERN} "${HDFS_BASE_DIR}/realtime/"

echo "Verification HDFS"
"${HDFS_CMD_BIN}" "${HDFS_FS_SUBCOMMAND}" -ls "${HDFS_BASE_DIR}/gtfs"
"${HDFS_CMD_BIN}" "${HDFS_FS_SUBCOMMAND}" -ls "${HDFS_BASE_DIR}/realtime"
