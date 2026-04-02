from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    hour,
    lower,
    minute,
    regexp_replace,
    row_number,
    second,
    split,
    trim,
    when,
    round as spark_round,
)
from pyspark.sql.window import Window

from config import DELAY_THRESHOLD_MINUTES, MAX_MATCH_WINDOW_MINUTES, SERVICE_DAY_ROLLOVER_HOUR, data_path
from spark_session import create_spark_session

GTFS_PATH = data_path("processed/gtfs/gtfs_schedule.parquet")
REALTIME_PATH = data_path("processed/realtime/realtime_passages.parquet")
OUTPUT_PATH = data_path("final/hybrid_transport_monitoring.parquet")


def normalize_stop_name(column_name: str):
    cleaned = lower(trim(col(column_name)))
    cleaned = regexp_replace(cleaned, "-", " ")
    cleaned = regexp_replace(cleaned, "'", " ")
    cleaned = regexp_replace(cleaned, r"[\\*\\(\\)]", " ")
    cleaned = regexp_replace(cleaned, r"\\s+", " ")
    return cleaned


def gtfs_time_to_seconds(column_name: str):
    return (
        split(col(column_name), ":").getItem(0).cast("int") * 3600
        + split(col(column_name), ":").getItem(1).cast("int") * 60
        + split(col(column_name), ":").getItem(2).cast("int")
    )


def service_seconds_from_timestamp(column_name: str):
    seconds = hour(column_name) * 3600 + minute(column_name) * 60 + second(column_name)
    return when(hour(column_name) < SERVICE_DAY_ROLLOVER_HOUR, seconds + 86400).otherwise(seconds)


def main() -> None:
    spark = create_spark_session("BuildHybridTransportMonitoring")

    gtfs = (
        spark.read.parquet(GTFS_PATH)
        .withColumn("stop_name_norm", normalize_stop_name("stop_name"))
        .withColumn("departure_time_seconds", gtfs_time_to_seconds("departure_time"))
        .filter(
            col("line_code").isNotNull()
            & col("stop_name_norm").isNotNull()
            & col("departure_time_seconds").isNotNull()
        )
    )

    realtime = (
        spark.read.parquet(REALTIME_PATH)
        .withColumn("stop_name_norm", normalize_stop_name("stop_point_name"))
        .withColumn("expected_departure_service_seconds", service_seconds_from_timestamp("expected_departure_ts"))
        .filter(
            col("line_code").isNotNull()
            & col("stop_name_norm").isNotNull()
            & col("expected_departure_service_seconds").isNotNull()
        )
    )

    joined = realtime.join(gtfs, on=["line_code", "stop_name_norm"], how="inner").withColumn(
        "time_diff_seconds",
        spark_abs(col("expected_departure_service_seconds") - col("departure_time_seconds")),
    )

    key_cols = [
        "source_file",
        "recorded_at_time",
        "monitoring_ref",
        "line_code",
        "stop_point_name",
        "expected_departure_time",
    ]

    best_match_window = Window.partitionBy(*key_cols).orderBy(col("time_diff_seconds").asc())

    departure_status = lower(col("departure_status"))
    arrival_status = lower(col("arrival_status"))

    final_df = (
        joined
        .withColumn("rn", row_number().over(best_match_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .filter(col("time_diff_seconds") <= MAX_MATCH_WINDOW_MINUTES * 60)
        .withColumn(
            "delay_minutes",
            spark_round(
                (col("expected_departure_service_seconds") - col("departure_time_seconds")) / 60.0,
                2,
            ),
        )
        .withColumn("is_delayed", col("delay_minutes") >= DELAY_THRESHOLD_MINUTES)
        .withColumn(
            "is_cancelled",
            (departure_status == "cancelled") | (arrival_status == "cancelled"),
        )
        .withColumn(
            "disruption_reason",
            when(col("is_cancelled"), "cancelled_service")
            .when(col("is_delayed"), "delay")
            .when((departure_status == "noreport") | (arrival_status == "noreport"), "missing_realtime_status")
            .otherwise("normal"),
        )
        .withColumn("is_disrupted", col("disruption_reason") != "normal")
        .withColumn("match_gap_minutes", spark_round(col("time_diff_seconds") / 60.0, 2))
        .select(
            "source_file",
            "recorded_at_time",
            "recorded_at_ts",
            "monitoring_ref",
            "line_code",
            "route_short_name",
            "stop_point_name",
            "expected_departure_time",
            "expected_departure_ts",
            "departure_time",
            "match_gap_minutes",
            "delay_minutes",
            "is_delayed",
            "is_cancelled",
            "is_disrupted",
            "disruption_reason",
        )
    )

    final_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print("Table hybride creee.")
    print("Sortie : {0}".format(OUTPUT_PATH))
    print("Nombre de lignes : {0}".format(final_df.count()))

    spark.stop()


if __name__ == "__main__":
    main()
