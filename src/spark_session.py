import os
import platform
import re
import subprocess
from typing import Optional

from pyspark.sql import SparkSession

from config import use_hdfs


def current_java_major() -> Optional[int]:
    java_bin = os.path.join(os.getenv("JAVA_HOME", ""), "bin", "java")
    command = [java_bin] if java_bin and os.path.exists(java_bin) else ["java"]

    try:
        result = subprocess.run(
            command + ["-version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None

    version_output = result.stderr or result.stdout
    match = re.search(r'version "(\d+)', version_output)
    if not match:
        return None
    return int(match.group(1))


def configure_local_java() -> None:
    if use_hdfs() or platform.system() != "Darwin":
        return

    java_major = current_java_major()
    if java_major == 17:
        return

    try:
        result = subprocess.run(
            ["/usr/libexec/java_home", "-v", "17"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return

    java_home = result.stdout.strip()
    if not java_home:
        return

    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = "{0}/bin:{1}".format(java_home, os.environ.get("PATH", ""))


def create_spark_session(app_name: str) -> SparkSession:
    configure_local_java()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", os.getenv("SPARK_TIMEZONE", "Europe/Paris"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    )

    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)
    elif not use_hdfs():
        builder = builder.master("local[2]")

    return builder.getOrCreate()
