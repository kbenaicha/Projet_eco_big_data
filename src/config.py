import os
from pathlib import Path
from typing import List, Optional


DEFAULT_MONITORING_REFS = [
    "STIF:StopPoint:Q:463158:",
    "STIF:StopArea:SP:71517:",
]

SERVICE_DAY_ROLLOVER_HOUR = int(os.getenv("SERVICE_DAY_ROLLOVER_HOUR", "4"))
MAX_MATCH_WINDOW_MINUTES = int(os.getenv("MAX_MATCH_WINDOW_MINUTES", "120"))
DELAY_THRESHOLD_MINUTES = float(os.getenv("DELAY_THRESHOLD_MINUTES", "5"))


def monitoring_refs() -> List[str]:
    value = os.getenv("MONITORING_REFS", "")
    if not value.strip():
        return DEFAULT_MONITORING_REFS
    return [item.strip() for item in value.split(",") if item.strip()]


def local_data_root() -> Path:
    return Path(os.getenv("DATA_ROOT", "data"))


def hdfs_data_root() -> str:
    group = os.getenv("GROUP")
    user = os.getenv("USER", "user")
    if group:
        default_root = "/education/{0}/{1}/project".format(group, user)
    else:
        default_root = "/user/{0}/project".format(user)
    return os.getenv("HDFS_BASE_DIR", default_root)


def use_hdfs() -> bool:
    return os.getenv("STORAGE_MODE", "local").strip().lower() == "hdfs"


def data_path(local_relative_path: str, hdfs_relative_path: Optional[str] = None) -> str:
    if use_hdfs():
        suffix = hdfs_relative_path or local_relative_path
        return "{0}/{1}".format(hdfs_data_root().rstrip("/"), suffix.lstrip("/"))
    return str(local_data_root() / local_relative_path)
