SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS `${hivevar:group}`;

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_hybrid_transport_monitoring_ext` (
  source_file STRING,
  recorded_at_time STRING,
  recorded_at_ts TIMESTAMP,
  monitoring_ref STRING,
  line_code STRING,
  route_short_name STRING,
  stop_point_name STRING,
  expected_departure_time STRING,
  expected_departure_ts TIMESTAMP,
  departure_time STRING,
  match_gap_minutes DOUBLE,
  delay_minutes DOUBLE,
  is_delayed BOOLEAN,
  is_cancelled BOOLEAN,
  is_disrupted BOOLEAN,
  disruption_reason STRING
)
STORED AS PARQUET
LOCATION '${hivevar:hdfs_base}/final/hybrid_transport_monitoring.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_line_performance_ext` (
  line_code STRING,
  route_short_name STRING,
  observations BIGINT,
  avg_delay_minutes DOUBLE,
  delay_rate_pct DOUBLE,
  disruption_rate_pct DOUBLE
)
STORED AS PARQUET
LOCATION '${hivevar:hdfs_base}/analytics/line_performance';

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_stop_performance_ext` (
  stop_point_name STRING,
  observations BIGINT,
  avg_delay_minutes DOUBLE,
  disruption_rate_pct DOUBLE
)
STORED AS PARQUET
LOCATION '${hivevar:hdfs_base}/analytics/stop_performance';

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_disruption_reasons_ext` (
  disruption_reason STRING,
  events BIGINT
)
STORED AS PARQUET
LOCATION '${hivevar:hdfs_base}/analytics/disruption_reasons';

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_network_overview_ext` (
  total_observations BIGINT,
  disrupted_observations BIGINT,
  cancelled_observations BIGINT,
  network_avg_delay_minutes DOUBLE
)
STORED AS PARQUET
LOCATION '${hivevar:hdfs_base}/analytics/network_overview';

CREATE TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_hybrid_transport_monitoring` (
  source_file STRING,
  recorded_at_time STRING,
  recorded_at_ts TIMESTAMP,
  monitoring_ref STRING,
  line_code STRING,
  route_short_name STRING,
  stop_point_name STRING,
  expected_departure_time STRING,
  expected_departure_ts TIMESTAMP,
  departure_time STRING,
  match_gap_minutes DOUBLE,
  delay_minutes DOUBLE,
  is_delayed BOOLEAN,
  is_cancelled BOOLEAN,
  is_disrupted BOOLEAN,
  disruption_reason STRING
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_line_performance` (
  line_code STRING,
  route_short_name STRING,
  observations BIGINT,
  avg_delay_minutes DOUBLE,
  delay_rate_pct DOUBLE,
  disruption_rate_pct DOUBLE
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_stop_performance` (
  stop_point_name STRING,
  observations BIGINT,
  avg_delay_minutes DOUBLE,
  disruption_rate_pct DOUBLE
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_disruption_reasons` (
  disruption_reason STRING,
  events BIGINT
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `${hivevar:group}`.`${hivevar:hiveUsername}_network_overview` (
  total_observations BIGINT,
  disrupted_observations BIGINT,
  cancelled_observations BIGINT,
  network_avg_delay_minutes DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE `${hivevar:group}`.`${hivevar:hiveUsername}_hybrid_transport_monitoring`
SELECT *
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_hybrid_transport_monitoring_ext`;

INSERT OVERWRITE TABLE `${hivevar:group}`.`${hivevar:hiveUsername}_line_performance`
SELECT *
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_line_performance_ext`;

INSERT OVERWRITE TABLE `${hivevar:group}`.`${hivevar:hiveUsername}_stop_performance`
SELECT *
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_stop_performance_ext`;

INSERT OVERWRITE TABLE `${hivevar:group}`.`${hivevar:hiveUsername}_disruption_reasons`
SELECT *
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_disruption_reasons_ext`;

INSERT OVERWRITE TABLE `${hivevar:group}`.`${hivevar:hiveUsername}_network_overview`
SELECT *
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_network_overview_ext`;

SELECT line_code, route_short_name, disruption_rate_pct, avg_delay_minutes
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_line_performance`
ORDER BY disruption_rate_pct DESC, avg_delay_minutes DESC
LIMIT 10;

SELECT stop_point_name, disruption_rate_pct, avg_delay_minutes
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_stop_performance`
ORDER BY disruption_rate_pct DESC, avg_delay_minutes DESC
LIMIT 10;

SELECT disruption_reason, events
FROM `${hivevar:group}`.`${hivevar:hiveUsername}_disruption_reasons`
ORDER BY events DESC;
