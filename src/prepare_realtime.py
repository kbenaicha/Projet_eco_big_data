import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, input_file_name, regexp_extract, size, to_timestamp, when

from config import data_path, local_data_root, use_hdfs
from spark_session import create_spark_session

RAW_PATH = data_path("raw/realtime/*.json")
OUTPUT_PATH = data_path("processed/realtime/realtime_passages.parquet")


def first_value(path: str):
    array_col = col(path)
    return when(size(array_col) > 0, array_col.getItem(0).getField("value"))


def local_raw_files() -> List[Path]:
    return sorted((local_data_root() / "raw" / "realtime").glob("*.json"))


def first_text(values):
    if isinstance(values, list) and values:
        first_item = values[0]
        if isinstance(first_item, dict):
            return first_item.get("value")
    return None


def nested_value(value):
    if isinstance(value, dict):
        return value.get("value")
    return None


def extract_records(file_path: Path, payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    deliveries = (
        payload.get("Siri", {})
        .get("ServiceDelivery", {})
        .get("StopMonitoringDelivery", [])
    )

    for delivery in deliveries:
        for visit in delivery.get("MonitoredStopVisit", []):
            journey = visit.get("MonitoredVehicleJourney", {})
            monitored_call = journey.get("MonitoredCall", {})

            yield {
                "source_file": file_path.name,
                "recorded_at_time": visit.get("RecordedAtTime"),
                "monitoring_ref": nested_value(visit.get("MonitoringRef")),
                "line_ref": nested_value(journey.get("LineRef")),
                "stop_point_name": first_text(monitored_call.get("StopPointName")),
                "expected_departure_time": monitored_call.get("ExpectedDepartureTime"),
                "departure_status": monitored_call.get("DepartureStatus"),
                "arrival_status": monitored_call.get("ArrivalStatus"),
            }


def transform_from_hdfs(spark) -> DataFrame:
    raw_df = spark.read.option("multiLine", True).json(RAW_PATH).withColumn("source_file", input_file_name())

    visits = (
        raw_df
        .withColumn("delivery", explode_outer(col("Siri.ServiceDelivery.StopMonitoringDelivery")))
        .withColumn("visit", explode_outer(col("delivery.MonitoredStopVisit")))
    )

    return visits.select(
        "source_file",
        col("visit.RecordedAtTime").alias("recorded_at_time"),
        col("visit.MonitoringRef.value").alias("monitoring_ref"),
        col("visit.MonitoredVehicleJourney.LineRef.value").alias("line_ref"),
        first_value("visit.MonitoredVehicleJourney.MonitoredCall.StopPointName").alias("stop_point_name"),
        col("visit.MonitoredVehicleJourney.MonitoredCall.ExpectedDepartureTime").alias("expected_departure_time"),
        col("visit.MonitoredVehicleJourney.MonitoredCall.DepartureStatus").alias("departure_status"),
        col("visit.MonitoredVehicleJourney.MonitoredCall.ArrivalStatus").alias("arrival_status"),
    )


def transform_from_local_files(spark) -> DataFrame:
    records = []
    for file_path in local_raw_files():
        with open(file_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        records.extend(extract_records(file_path, payload))

    if not records:
        raise RuntimeError("Aucun snapshot JSON trouve dans data/raw/realtime")

    return spark.createDataFrame(records)


def main() -> None:
    spark = create_spark_session("PrepareRealtime")
    realtime = transform_from_hdfs(spark) if use_hdfs() else transform_from_local_files(spark)

    extracted_code = regexp_extract(col("line_ref"), r"(C[0-9]+)", 1)

    realtime = (
        realtime
        .filter(col("monitoring_ref").isNotNull() & col("stop_point_name").isNotNull())
        .withColumn(
            "line_code",
            when(extracted_code != "", extracted_code).otherwise(col("line_ref")),
        )
        .withColumn("recorded_at_ts", to_timestamp("recorded_at_time"))
        .withColumn("expected_departure_ts", to_timestamp("expected_departure_time"))
        .select(
            "source_file",
            "recorded_at_time",
            "recorded_at_ts",
            "monitoring_ref",
            "line_ref",
            "line_code",
            "stop_point_name",
            "expected_departure_time",
            "expected_departure_ts",
            "departure_status",
            "arrival_status",
        )
    )

    realtime.write.mode("overwrite").parquet(OUTPUT_PATH)

    print("Preparation temps reel terminee.")
    print("Sortie : {0}".format(OUTPUT_PATH))
    print("Nombre de lignes : {0}".format(realtime.count()))

    spark.stop()


if __name__ == "__main__":
    main()
