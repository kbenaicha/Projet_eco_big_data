from pyspark.sql.functions import avg, col, count, desc, round as spark_round, sum as spark_sum, when

from config import data_path
from spark_session import create_spark_session

INPUT_PATH = data_path("final/hybrid_transport_monitoring.parquet")
OUTPUT_DIR = data_path("analytics")


def ratio(column_name: str):
    return spark_round(avg(when(col(column_name), 1.0).otherwise(0.0)) * 100, 2)


def main() -> None:
    spark = create_spark_session("BuildTransportAnalytics")
    df = spark.read.parquet(INPUT_PATH)

    line_performance = (
        df.groupBy("line_code", "route_short_name")
        .agg(
            count("*").alias("observations"),
            spark_round(avg("delay_minutes"), 2).alias("avg_delay_minutes"),
            ratio("is_delayed").alias("delay_rate_pct"),
            ratio("is_disrupted").alias("disruption_rate_pct"),
        )
        .orderBy(desc("disruption_rate_pct"), desc("avg_delay_minutes"))
    )

    stop_performance = (
        df.groupBy("stop_point_name")
        .agg(
            count("*").alias("observations"),
            spark_round(avg("delay_minutes"), 2).alias("avg_delay_minutes"),
            ratio("is_disrupted").alias("disruption_rate_pct"),
        )
        .orderBy(desc("disruption_rate_pct"), desc("avg_delay_minutes"))
    )

    disruption_reasons = (
        df.groupBy("disruption_reason")
        .agg(count("*").alias("events"))
        .orderBy(desc("events"))
    )

    network_overview = df.agg(
        count("*").alias("total_observations"),
        spark_sum(when(col("is_disrupted"), 1).otherwise(0)).alias("disrupted_observations"),
        spark_sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_observations"),
        spark_round(avg("delay_minutes"), 2).alias("network_avg_delay_minutes"),
    )

    line_performance.write.mode("overwrite").parquet("{0}/line_performance".format(OUTPUT_DIR))
    stop_performance.write.mode("overwrite").parquet("{0}/stop_performance".format(OUTPUT_DIR))
    disruption_reasons.write.mode("overwrite").parquet("{0}/disruption_reasons".format(OUTPUT_DIR))
    network_overview.write.mode("overwrite").parquet("{0}/network_overview".format(OUTPUT_DIR))

    print("Analytics generees.")
    print("Sorties : {0}".format(OUTPUT_DIR))

    spark.stop()


if __name__ == "__main__":
    main()
