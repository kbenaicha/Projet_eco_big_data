import os

from pyspark.sql import SparkSession

from config import use_hdfs


def create_spark_session(app_name: str) -> SparkSession:
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
