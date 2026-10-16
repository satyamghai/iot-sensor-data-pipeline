# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Staging table 
# Mirrors the Silver schema.
spark.sql("""
          CREATE TABLE IF NOT EXISTS iot_sensors.gold.gold_staging
          COMMENT 'Temporary staging : new silver rows land here before gold aggregation'
          AS SELECT * FROM iot_sensors.silver.silver_iot_data WHERE 1=0
          """)

# COMMAND ----------

# One row per device. Gives a complete performance picture for each sensor.
spark.sql("""
          CREATE TABLE IF NOT EXISTS iot_sensors.gold.gold_device_metrics(
              device_id STRING,
              location STRING,
              sensor_type STRING,
              total_readings LONG,
              avg_temperature DOUBLE,
              max_temperature DOUBLE,
              min_temperature DOUBLE,
              avg_humidity DOUBLE,
              active_readings LONG,
              error_readings LONG,
              off_readings LONG,
              error_rate_pct DOUBLE,
              total_alert_events LONG,
              alert_rate_pct DOUBLE,
              first_seen TIMESTAMP,
              last_seen TIMESTAMP,
              updated_at TIMESTAMP
          )
          COMMENT 'Gold : lifetime performance KPIs aggregated per device'
          """)

# COMMAND ----------


# One row per date + hour + location combination. Good for time-series charts.
spark.sql("""
          CREATE TABLE IF NOT EXISTS iot_sensors.gold.gold_hourly_trends(
              event_date DATE,
              event_hour INT,
              location STRING,
              reading_count LONG,
              avg_temperature DOUBLE,
              avg_humidity DOUBLE,
              max_temperature DOUBLE,
              alert_count LONG,
              error_count LONG,
              updated_at TIMESTAMP
          )
          COMMENT 'Gold : hourly sensor trends grouped by date and location'
            """)

# COMMAND ----------

# One row per date + device combination. Shows which days had the most problems.
spark.sql("""
          CREATE TABLE IF NOT EXISTS iot_sensors.gold.gold_daily_alert_summary(
              event_date DATE,
              device_id STRING,
              location STRING,
              daily_readings LONG,
              daily_alerts LONG,
              daily_errors LONG,
              max_temperature DOUBLE,
              avg_temperature DOUBLE,
              alert_rate_pct DOUBLE,
              updated_at TIMESTAMP
          )
          COMMENT 'Gold : daily alert and error counts per device'
          """)

# COMMAND ----------

# One row per device that is MEDIUM, HIGH, or CRITICAL risk.
# Risk score formula: 50% error rate + 30% alert rate + 20% temperature level.
spark.sql("""
          CREATE TABLE IF NOT EXISTS iot_sensors.gold.gold_high_risk_devices(
              device_id STRING,
              location STRING,
              risk_score DOUBLE,
              risk_label STRING,
              total_alerts LONG,
              total_errors LONG,
              error_rate_pct DOUBLE,
              alert_rate_pct DOUBLE,
              avg_temperature DOUBLE,
              last_error_ts TIMESTAMP,
              updated_at TIMESTAMP
          )
          COMMENT 'Gold : devices classified as high-risk based on error and alert rates'
          """)

# COMMAND ----------

print("All gold tables are ready")

# COMMAND ----------

# Set up a stream that reads from the Silver Delta table
silver_stream=(
    spark.readStream
    .format("delta")
    .table("iot_sensors.silver.silver_iot_data")
)

# COMMAND ----------

# Write new Silver rows into the Gold staging table
gold_staging_query=(
    silver_stream.writeStream
    .format("delta")
    .option("checkpointLocation","/Volumes/iot_sensors/default/rawdata/checkpoint_gold_staging")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("iot_sensors.gold.gold_staging")
)

# COMMAND ----------

print("New silver rows have been written to gold staging")

# COMMAND ----------

print(f"Total rows in gold staging : {spark.table("iot_sensors.gold.gold_staging").count()}")

# COMMAND ----------

# Load the full Silver table as a static DataFrame for aggregation
silver_full_df=spark.table("iot_sensors.silver.silver_iot_data")

# COMMAND ----------

# Get the current timestamp to stamp all Gold rows with when they were last updated
current_time=current_timestamp()

# COMMAND ----------

total_silver_rows=silver_full_df.count()

# COMMAND ----------

print(f"Full silver table loaded. Total rows : {total_silver_rows}")

# COMMAND ----------

# Aggregate Silver grouped by device_id, location, and sensor_type
spark.sql("""
          CREATE OR REPLACE TEMP VIEW device_metrics AS 
          WITH metrics AS (
              SELECT device_id,
              location,
              sensor_type,
              COUNT(*) AS total_readings,
              ROUND(AVG(temperature),2) AS avg_temperature,
              ROUND(MAX(temperature),2) AS max_temperature,
              ROUND(MIN(temperature),2) AS min_temperature,
              ROUND(AVG(humidity),2) AS avg_humidity,
              SUM(CASE WHEN device_status='ON' THEN 1 ELSE 0 END) AS active_readings,
              SUM(CASE WHEN device_status='ERROR' THEN 1 ELSE 0 END) AS error_readings,
              SUM(CASE WHEN device_status='OFF' THEN 1 ELSE 0 END) AS off_readings,
              SUM(CAST(is_alert AS INT)) AS total_alert_events,
              MIN(event_ts) AS first_seen,
              MAX(event_ts) AS last_seen
              FROM iot_sensors.silver.silver_iot_data 
              GROUP BY device_id,location,sensor_type
          )
          SELECT *,
          ROUND((error_readings/total_readings)*100,2) AS error_rate_pct,
          ROUND((total_alert_events/total_readings)*100,2) AS alert_rate_pct,
          CURRENT_TIMESTAMP() AS updated_at
          FROM metrics
          """)

# COMMAND ----------

device_metrics_df=spark.table("device_metrics")

# COMMAND ----------

# MERGE into gold_device_metrics: update the row if device exists, insert if it is new
spark.sql("""
          MERGE INTO iot_sensors.gold.gold_device_metrics AS source 
          USING device_metrics AS target
          ON source.device_id=target.device_id
          AND source.location=target.location
          AND source.sensor_type=target.sensor_type
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_sensors.gold.gold_device_metrics;

# COMMAND ----------

print(f"gold_device_metrics updated : ({device_metrics_df.count()} devices)")

# COMMAND ----------

print("Computing gold hourly trends......")

# COMMAND ----------

# Aggregate Silver grouped by date, hour, and location
spark.sql("""
          CREATE OR REPLACE TEMP VIEW hourly_trends AS 
          SELECT event_date,
          event_hour,
          location,
          COUNT(*) AS reading_count,
          ROUND(AVG(temperature),2) AS avg_temperature,
          ROUND(AVG(humidity),2) AS avg_humidity,
          ROUND(MAX(temperature),2) AS max_temperature,
          SUM(CAST(is_alert AS INT)) AS alert_count,
          SUM(CASE WHEN device_status='ERROR' THEN 1 ELSE 0 END) AS error_count,
          CURRENT_TIMESTAMP() AS updated_at 
          FROM iot_sensors.silver.silver_iot_data
          GROUP BY event_date,event_hour,location
          """)

# COMMAND ----------

hourly_trends_df=spark.table("hourly_trends")

# COMMAND ----------

spark.sql("""
          MERGE INTO iot_sensors.gold.gold_hourly_trends AS source
          USING hourly_trends AS target 
          ON source.event_date=target.event_date
          AND source.event_hour=target.event_hour 
          AND source.location=target.location
          WHEN MATCHED THEN UPDATE SET * 
          WHEN NOT MATCHED THEN INSERT *
          """)

# COMMAND ----------

print(f"gold_hourly_trends updated. {hourly_trends_df.count()} rows")

# COMMAND ----------

print("Computing gold_daily_alert_summary....")

# COMMAND ----------

# Aggregate Silver grouped by date and device
spark.sql("""
          CREATE OR REPLACE TEMP VIEW daily_alerts AS 
          WITH daily_metrics AS (
          SELECT event_date,
          device_id,
          location,
          COUNT(*) AS daily_readings,
          SUM(CAST(is_alert AS INT)) AS daily_alerts,
          SUM(CASE WHEN device_status='ERROR' THEN 1 ELSE 0 END) AS daily_errors,
          ROUND(MAX(temperature),2) AS max_temperature,
          ROUND(AVG(temperature),2) AS avg_temperature
          FROM iot_sensors.silver.silver_iot_data
          GROUP BY event_date,device_id,location
          )
          SELECT *,
          ROUND((daily_alerts/daily_readings)*100,2) AS alert_rate_pct,
          CURRENT_TIMESTAMP() AS updated_at 
          FROM daily_metrics
          """)

# COMMAND ----------

daily_alerts_df=spark.table("daily_alerts")

# COMMAND ----------

spark.sql("""
          MERGE INTO iot_sensors.gold.gold_daily_alert_summary AS target 
          USING daily_alerts AS source
          ON target.event_date=source.event_date
          AND target.device_id=source.device_id 
          AND target.location=source.location
          WHEN MATCHED THEN UPDATE SET * 
          WHEN NOT MATCHED THEN INSERT *
          """)

# COMMAND ----------

print(f"gold_daily_alert_summary updated ({daily_alerts_df.count()} rows)")

# COMMAND ----------

print("Computing gold_high_risk_devices....")

# COMMAND ----------

device_metrics_for_risk_df=spark.table("iot_sensors.gold.gold_device_metrics")

# COMMAND ----------

# Calculate the risk score and label for every device
spark.sql("""
          CREATE OR REPLACE TEMP VIEW all_devices_with_risk AS 
          WITH risk_calculated AS (
              SELECT *,
              ROUND(error_rate_pct*0.5+alert_rate_pct*0.3+avg_temperature*0.2,2) AS risk_score 
              FROM iot_sensors.gold.gold_device_metrics
          )
          SELECT *,
          CASE WHEN risk_score>=50 THEN 'CRITICAL'
          WHEN risk_score>=25 THEN 'HIGH'
          WHEN risk_score>=10 THEN 'MEDIUM'
          ELSE 'LOW' 
          END AS risk_label
          FROM risk_calculated
          """)

# COMMAND ----------

# Create a Gold summary table showing the number of distinct devices in each risk category.
spark.sql("""
CREATE OR REPLACE TABLE iot_sensors.gold.gold_device_risk_distribution
SELECT
    risk_label,
    COUNT(DISTINCT device_id) AS device_count
FROM all_devices_with_risk
GROUP BY risk_label
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_sensors.gold.gold_device_risk_distribution

# COMMAND ----------

# Keep only devices that are at MEDIUM risk or worse
spark.sql("""
          CREATE OR REPLACE TEMP VIEW high_risk_devices AS 
          SELECT * FROM all_devices_with_risk
          WHERE risk_label IN ('CRITICAL','HIGH','MEDIUM')
          """)

# COMMAND ----------

# Find the most recent ERROR reading timestamp for each device
# This tells us when each device last had a problem
spark.sql("""
          CREATE OR REPLACE TEMP VIEW last_error_timestamp AS
          SELECT device_id,MAX(event_ts) AS last_error_ts
          FROM iot_sensors.silver.silver_iot_data
          WHERE device_status='ERROR'
          GROUP BY device_id
          """)

# COMMAND ----------

# Join the risk scores with the last error timestamp
spark.sql("""
          CREATE OR REPLACE TEMP VIEW high_risk_with_error_time AS
          SELECT
          h.device_id,
          h.location,
          h.risk_score,
          h.risk_label,
          h.total_alert_events AS total_alerts,
          h.error_readings AS total_errors,
          h.error_rate_pct,
          h.alert_rate_pct,
          h.avg_temperature,
          e.last_error_ts,
          CURRENT_TIMESTAMP() AS updated_at
          FROM high_risk_devices h
          LEFT JOIN last_error_timestamp e
          ON h.device_id=e.device_id
          """)

# COMMAND ----------

high_risk_with_error_time_df=spark.table("high_risk_with_error_time")

# COMMAND ----------

spark.sql("""
          MERGE INTO iot_sensors.gold.gold_high_risk_devices AS target
          USING high_risk_with_error_time AS source 
          ON target.device_id=source.device_id
          WHEN MATCHED THEN UPDATE SET * 
          WHEN NOT MATCHED THEN INSERT *
          """)

# COMMAND ----------

print(f"gold_high_risk_devices updated. ({high_risk_with_error_time_df.count()} at -risk devices)")

# COMMAND ----------

# Device metrics — sorted to show the most problematic devices at the top
print("gold_device_metrics (sorted by error rate,worst first):")
spark.table("iot_sensors.gold.gold_device_metrics").orderBy("error_rate_pct",ascending=False).show()

# COMMAND ----------

# High-risk device classification — most critical at the top
print("gold_high_risk_devices (sorted by risk score,worst first) :")
spark.table("iot_sensors.gold.gold_high_risk_devices").orderBy("risk_score",ascending=False).show()

# COMMAND ----------

# Daily alert summary — worst days at the top
print("gold_daily_alert_summary (top 20 days with most alerts):")
spark.table("iot_sensors.gold.gold_daily_alert_summary").orderBy(col("daily_alerts").desc()).limit(20).show()

# COMMAND ----------

# Hourly trends — first 30 rows as a sample
print("gold_hourly_trends (sample - first 30 rows ordered by date and hour):")
spark.table("iot_sensors.gold.gold_hourly_trends").orderBy("event_date","event_hour","location").limit(30).show()

# COMMAND ----------

