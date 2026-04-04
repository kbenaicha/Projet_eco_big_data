#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

resolve_cluster_cmd() {
  local fallback_name="$1"
  shift

  while [ "$#" -gt 0 ]; do
    if [ -x "$1" ]; then
      printf '%s\n' "$1"
      return 0
    fi
    shift
  done

  if command -v "${fallback_name}" >/dev/null 2>&1; then
    command -v "${fallback_name}"
    return 0
  fi

  return 1
}

if ! command -v hdfs >/dev/null 2>&1; then
  echo "La commande hdfs est introuvable. Lance ce script depuis le cluster." >&2
  exit 1
fi

if ! command -v oozie >/dev/null 2>&1; then
  echo "La commande oozie est introuvable. Lance ce script depuis un noeud avec la CLI Oozie." >&2
  exit 1
fi

if [ ! -d "data/raw/gtfs/IDFM-gtfs" ] || [ ! -d "data/raw/realtime" ]; then
  echo "Le dossier data/raw est incomplet. Il faut data/raw/gtfs/IDFM-gtfs et data/raw/realtime." >&2
  exit 1
fi

GROUP_NAME="${GROUP_NAME:-${GROUP:-default}}"
USER_NAME="${USER_NAME:-${USER:-user}}"
if [ -n "${GROUP_NAME}" ] && [ "${GROUP_NAME}" != "default" ]; then
  DEFAULT_HDFS_BASE_DIR="/education/${GROUP_NAME}/${USER_NAME}/project"
else
  DEFAULT_HDFS_BASE_DIR="/user/${USER_NAME}/project"
fi
HDFS_BASE_DIR="${HDFS_BASE_DIR:-${DEFAULT_HDFS_BASE_DIR}}"
OOZIE_APP_HDFS_DIR="${OOZIE_APP_HDFS_DIR:-${HDFS_BASE_DIR}/oozie_app}"
OOZIE_LOCAL_BUILD_DIR="${OOZIE_LOCAL_BUILD_DIR:-build/oozie}"
UPLOAD_RAW_FIRST="${UPLOAD_RAW_FIRST:-true}"
OOZIE_URL="${OOZIE_URL:-}"
NAME_NODE="${NAME_NODE:-}"
JOB_TRACKER="${JOB_TRACKER:-}"
QUEUE_NAME="${QUEUE_NAME:-default}"
DEFAULT_SPARK_SUBMIT_CMD="$(resolve_cluster_cmd spark-submit \
  /usr/hdp/3.1.0.0-78/spark2/bin/spark-submit \
  /usr/hdp/current/spark2-client/bin/spark-submit \
  /usr/bin/spark-submit)"
SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD:-${DEFAULT_SPARK_SUBMIT_CMD}}"
PYSPARK_PYTHON_BIN="${PYSPARK_PYTHON:-python3}"
PYSPARK_DRIVER_PYTHON_BIN="${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON_BIN}}"
DEFAULT_HIVE_CMD="$(resolve_cluster_cmd beeline \
  /usr/hdp/3.1.0.0-78/hive/bin/beeline \
  /usr/hdp/current/hive-client/bin/beeline \
  /usr/bin/beeline)"
HIVE_CMD="${HIVE_CMD:-${DEFAULT_HIVE_CMD}}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-}"
HIVE_GROUP_NAME="${HIVE_GROUP_NAME:-${GROUP_NAME}}"
HIVE_USER_NAME="${HIVE_USER_NAME:-${USER_NAME}}"
DEFAULT_HIVE_USERNAME="$(printf '%s' "${HIVE_USER_NAME}_transport" | tr '.-' '__')"
HIVE_USERNAME="${HIVE_USERNAME:-${DEFAULT_HIVE_USERNAME}}"

if [ -z "${OOZIE_URL}" ]; then
  echo "OOZIE_URL est obligatoire, par exemple http://host:11000/oozie" >&2
  exit 1
fi

if [ -z "${NAME_NODE}" ]; then
  echo "NAME_NODE est obligatoire, par exemple hdfs://nameservice1" >&2
  exit 1
fi

if [ -z "${JOB_TRACKER}" ]; then
  echo "JOB_TRACKER est obligatoire, par exemple rm-host:8032" >&2
  exit 1
fi

if ! printf '%s' "${JOB_TRACKER}" | grep -Eq '^[A-Za-z0-9._-]+:[0-9]+$'; then
  echo "JOB_TRACKER invalide: ${JOB_TRACKER}" >&2
  echo "Format attendu: <host>:<port>, par exemple yarn-rm-1.au.adaltas.cloud:8032" >&2
  exit 1
fi

STAGE_DIR="${PROJECT_ROOT}/${OOZIE_LOCAL_BUILD_DIR}"
APP_STAGE_DIR="${STAGE_DIR}/app"
BUNDLE_STAGE_DIR="${STAGE_DIR}/bundle"
ARCHIVE_PATH="${APP_STAGE_DIR}/project_bundle.tar.gz"
JOB_PROPERTIES_PATH="${STAGE_DIR}/job.properties"

rm -rf "${STAGE_DIR}"
mkdir -p "${APP_STAGE_DIR}" "${BUNDLE_STAGE_DIR}/data"

cp "oozie/workflow.xml" "${APP_STAGE_DIR}/workflow.xml"
cp -R "src" "${BUNDLE_STAGE_DIR}/"
cp -R "scripts" "${BUNDLE_STAGE_DIR}/"
cp -R "hive" "${BUNDLE_STAGE_DIR}/"
cp -R "data/raw" "${BUNDLE_STAGE_DIR}/data/"
cp "README.md" "${BUNDLE_STAGE_DIR}/"
cp "requirements.txt" "${BUNDLE_STAGE_DIR}/"

(
  cd "${BUNDLE_STAGE_DIR}"
  tar -czf "${ARCHIVE_PATH}" "src" "scripts" "hive" "data" "README.md" "requirements.txt"
)

hdfs dfs -mkdir -p "${OOZIE_APP_HDFS_DIR}"
hdfs dfs -put -f "${APP_STAGE_DIR}/workflow.xml" "${OOZIE_APP_HDFS_DIR}/workflow.xml"
hdfs dfs -put -f "${ARCHIVE_PATH}" "${OOZIE_APP_HDFS_DIR}/project_bundle.tar.gz"

OOZIE_APP_URI="${NAME_NODE%/}${OOZIE_APP_HDFS_DIR}"
PROJECT_ARCHIVE_URI="${OOZIE_APP_URI}/project_bundle.tar.gz#project"

cat > "${JOB_PROPERTIES_PATH}" <<EOF
nameNode=${NAME_NODE}
jobTracker=${JOB_TRACKER}
queueName=${QUEUE_NAME}
oozie.use.system.libpath=true
oozie.wf.application.path=${OOZIE_APP_URI}
projectArchive=${PROJECT_ARCHIVE_URI}
group=${GROUP_NAME}
user_name=${USER_NAME}
hdfsBaseDir=${HDFS_BASE_DIR}
uploadRawFirst=${UPLOAD_RAW_FIRST}
hiveGroupName=${HIVE_GROUP_NAME}
hiveUserName=${HIVE_USER_NAME}
hiveUsername=${HIVE_USERNAME}
sparkSubmitCmd=${SPARK_SUBMIT_CMD}
pysparkPython=${PYSPARK_PYTHON_BIN}
pysparkDriverPython=${PYSPARK_DRIVER_PYTHON_BIN}
hiveCmd=${HIVE_CMD}
hiveJdbcUrl=${HIVE_JDBC_URL}
EOF

echo "Application Oozie chargee dans ${OOZIE_APP_HDFS_DIR}"
echo "Job properties : ${JOB_PROPERTIES_PATH}"
echo "sparkSubmitCmd=${SPARK_SUBMIT_CMD}"
echo "hiveCmd=${HIVE_CMD}"
oozie job -oozie "${OOZIE_URL}" -config "${JOB_PROPERTIES_PATH}" -run
