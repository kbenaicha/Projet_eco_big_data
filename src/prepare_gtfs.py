from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract, when

from config import data_path
from spark_session import create_spark_session

ROUTES_PATH = data_path("raw/gtfs/IDFM-gtfs/routes.txt", "raw/gtfs/routes.txt")
TRIPS_PATH = data_path("raw/gtfs/IDFM-gtfs/trips.txt", "raw/gtfs/trips.txt")
STOPS_PATH = data_path("raw/gtfs/IDFM-gtfs/stops.txt", "raw/gtfs/stops.txt")
STOP_TIMES_PATH = data_path("raw/gtfs/IDFM-gtfs/stop_times.txt", "raw/gtfs/stop_times.txt")

OUTPUT_PATH = data_path("processed/gtfs/gtfs_schedule.parquet")


def select_existing_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    return df.select(*[column for column in columns if column in df.columns])


def main() -> None:
    spark = create_spark_session("PrepareGTFS")

    routes = select_existing_columns(
        spark.read.option("header", True).csv(ROUTES_PATH),
        ["route_id", "route_short_name"],
    )
    trips = select_existing_columns(
        spark.read.option("header", True).csv(TRIPS_PATH),
        ["route_id", "trip_id"],
    )
    stops = select_existing_columns(
        spark.read.option("header", True).csv(STOPS_PATH),
        ["stop_id", "stop_name"],
    )
    stop_times = select_existing_columns(
        spark.read.option("header", True).csv(STOP_TIMES_PATH),
        ["trip_id", "stop_id", "departure_time"],
    )

    extracted_code = regexp_extract(col("route_id"), r"(C[0-9]+)", 1)

    gtfs = (
        trips
        .join(routes, on="route_id", how="left")
        .join(stop_times, on="trip_id", how="inner")
        .join(stops, on="stop_id", how="left")
        .withColumn(
            "line_code",
            when(extracted_code != "", extracted_code).otherwise(col("route_short_name")),
        )
        .select(
            "line_code",
            "route_short_name",
            "trip_id",
            "stop_name",
            "departure_time",
        )
        .filter(col("line_code").isNotNull() & col("stop_name").isNotNull())
        .dropDuplicates()
    )

    gtfs.write.mode("overwrite").parquet(OUTPUT_PATH)

    print("Preparation GTFS terminee.")
    print("Sortie : {0}".format(OUTPUT_PATH))
    print("Nombre de lignes : {0}".format(gtfs.count()))

    spark.stop()


if __name__ == "__main__":
    main()
