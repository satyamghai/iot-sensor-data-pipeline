# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Staging table 
# Mirrors the Bronze schema exactly.
spark.sql("""
CREATE TABLE IF NOT EXISTS iot_sensors.silver.silver_staging(
    device_id STRING,
    timestamp STRING,
    temperature STRING,
    humidity STRING,
    device_status STRING,
    location STRING,
    sensor_type STRING,
    ingestion_time TIMESTAMP,
    source_file STRING,
    _rescued_data STRING
)
COMMENT 'Temporary staging area: new Bronze rows land here before quality checks'
""")

# COMMAND ----------

# Silver clean table 
# Notice the columns now have proper types (DOUBLE, TIMESTAMP, DATE, INT, BOOLEAN)
# instead of STRING — this is the main difference from Bronze.
spark.sql("""
CREATE TABLE IF NOT EXISTS iot_sensors.silver.silver_iot_data (
    device_id STRING,
    event_ts TIMESTAMP,
    event_date DATE,
    event_hour INT,
    temperature DOUBLE,
    humidity DOUBLE,
    device_status STRING,
    location STRING,
    sensor_type STRING,
    is_alert BOOLEAN,
    source_file STRING,
    bronze_ingest TIMESTAMP,
    silver_updated TIMESTAMP
)
COMMENT 'Silver table: cleaned and validated IoT sensor data'
""")

# COMMAND ----------

# Quarantine table 
# Bad records are stored here along with the reason they were rejected.
spark.sql("""
CREATE TABLE IF NOT EXISTS iot_sensors.silver.silver_quarantine (
    device_id STRING,
    timestamp STRING,
    temperature STRING,
    humidity STRING,
    device_status STRING,
    location STRING,
    sensor_type STRING,
    source_file STRING,
    bronze_ingest TIMESTAMP,
    rejection_reason STRING,
    quarantine_time TIMESTAMP
)
COMMENT 'Quarantine table: records that failed data quality checks'
""")

# COMMAND ----------

staging_history_df=spark.sql("DESCRIBE HISTORY iot_sensors.silver.silver_staging")

# COMMAND ----------

staging_version_before_stream=staging_history_df.select(max("version")).first()[0]

# COMMAND ----------

if staging_version_before_stream is None:
    staging_version_before_stream=-1

# COMMAND ----------

# Save the Silver row count before processing
row_count_before_processing=spark.table("iot_sensors.silver.silver_iot_data").count()

# COMMAND ----------

print(f"Staging table version before stream runs : {staging_version_before_stream}")
print(f"Rows in silver before this run : {row_count_before_processing}")

# COMMAND ----------

staging_history_df=spark.sql("DESCRIBE HISTORY iot_sensors.silver.silver_staging")
display(staging_history_df)

# COMMAND ----------

# We read from Bronze as a Delta stream. This automatically picks up only the rows
# that were added to Bronze since the last time this notebook ran (tracked via checkpoint).
bronze_stream=(
    spark.readStream
    .format("delta")
    .table("iot_sensors.bronze.bronze_iot_data")
)

# COMMAND ----------

# Write all new Bronze rows into the staging table (simple append, no transformation yet)
staging_write_query=(
    bronze_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation","/Volumes/iot_sensors/default/rawdata/checkpoint_staging")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("iot_sensors.silver.silver_staging")
)
staging_write_query.awaitTermination()

# COMMAND ----------

# Validate the Silver staging data by counting records for each source file.
spark.table("iot_sensors.silver.silver_staging") \
    .groupBy("source_file") \
    .count() \
    .orderBy("source_file") \
    .show()

# COMMAND ----------

# Get the staging table's version number after the stream finished
staging_history_df=spark.sql("DESCRIBE HISTORY iot_sensors.silver.silver_staging")
staging_version_after_stream=staging_history_df.select(max("version")).first()[0]

# COMMAND ----------

print(f"Staging version before stream : {staging_version_before_stream}")
print(f"Staging version after stream : {staging_version_after_stream}")

# COMMAND ----------

# If the version did not change, the stream found no new files in Bronze
# This means Silver is already fully up to date — nothing to do
if staging_version_before_stream==staging_version_after_stream:
    print("No new rows were written to staging.Silver is already up to date")

# COMMAND ----------

# Read ONLY the rows that were added in this run.
# We do this by reading each version individually and taking the union.
new_bronze_rows_df=(
    spark.read
    .format("delta")
    .option("versionAsOf",staging_version_after_stream)
    .table("iot_sensors.silver.silver_staging")
    .subtract(
        spark.read
        .format("delta")
        .option("versionAsOf",staging_version_before_stream)
        .table("iot_sensors.silver.silver_staging")
    )
)

# COMMAND ----------

new_row_count=new_bronze_rows_df.count()

# COMMAND ----------

print(f"New rows to process in this run :",new_row_count)

# COMMAND ----------

# In Bronze everything is a STRING. Here we cast each column to the type it should be.
casted_df=(
    new_bronze_rows_df
    .withColumn("temperature_cast",col("temperature").cast(DoubleType()))
    .withColumn("humidity_cast",col("humidity").cast(DoubleType()))
    .withColumn("event_ts",to_timestamp(col("timestamp"),"yyyy-MM-dd HH:mm:ss"))
)

# COMMAND ----------

print(f"Columns cast to correct types. Rows in batch : {casted_df.count()}")

# COMMAND ----------

casted_df.createOrReplaceTempView("casted_data")

# COMMAND ----------

# We add a new column called `_rejection_reason`. For rows that pass all checks
# this column is null. For rows that fail, it contains a short text explaining why.
spark.sql("""
          CREATE OR REPLACE TEMP VIEW tagged_data AS 
          SELECT *,CASE
          WHEN device_id IS NULL OR TRIM(device_id)='' THEN 'MISSING_DEVICE_ID'
          WHEN event_ts IS NULL THEN 'INVALID_TIMESTAMP'
          WHEN temperature_cast IS NULL THEN 'NULL TEMPERATURE'
          WHEN humidity_cast IS NULL THEN 'NULL HUMIDITY'
          WHEN temperature_cast<-50 OR temperature_cast>100 THEN "OUT_OF_RANGE_TEMP"
          WHEN humidity_cast<0 OR humidity_cast>100 THEN "OUT_OF_RANGE_HUM"
          WHEN device_status NOT IN ('ON','OFF','ERROR') THEN 'INVALID_STATUS'
          ELSE NULL 
          END AS rejection_reason
          FROM casted_data 
          """)

# COMMAND ----------

tagged_df=spark.table("tagged_data")

# COMMAND ----------

# Split Into Clean Rows and Quarantine Rows

# COMMAND ----------

# Clean rows: rejection_reason is null (they passed every quality check)
clean_rows_df=tagged_df.filter(col("rejection_reason").isNull())

# COMMAND ----------

# Bad rows: rejection_reason has a value (they failed at least one check)
quarantine_rows_df=tagged_df.filter(col("rejection_reason").isNotNull())

# COMMAND ----------

clean_count=clean_rows_df.count()
quarantine_count=quarantine_rows_df.count()

# COMMAND ----------

print(f"Rows that passed quality checks : {clean_count}")
print(f"Rows that failed quality checks : {quarantine_count}")

# COMMAND ----------

# Now we derive the extra columns (`event_date`, `event_hour`, `is_alert`),
# rename the cast columns, remove duplicates, and produce the final Silver DataFrame.
silver_ready_df=(
    clean_rows_df
    .withColumn("event_date",to_date(col("event_ts")))
    .withColumn("event_hour",hour(col("event_ts")))
    .withColumn("is_alert",col("temperature_cast")>50)
    .dropDuplicates(["device_id","event_ts"])
    .select(
        col("device_id"),
        col("event_ts"),
        col("event_date"),
        col("event_hour"),
        col("temperature_cast").alias("temperature"),
        col("humidity_cast").alias("humidity"),
        col("device_status"),
        col("location"),
        col("sensor_type"),
        col("is_alert"),
        col("source_file"),
        col("ingestion_time").alias("bronze_ingest"),
        current_timestamp().alias("silver_updated")  
    )
)

# COMMAND ----------

print(f"Silver ready rows : {silver_ready_df.count()}")

# COMMAND ----------

silver_ready_df.createOrReplaceTempView("silver_ready_data")

# COMMAND ----------

# We use MERGE (also called upsert) instead of a plain append.
# MERGE checks if a row with the same `device_id` + `event_ts` already exists:
# If it does exist → update all columns with the new values
# If it does not exist → insert it as a brand new row
spark.sql("""
          MERGE INTO iot_sensors.silver.silver_iot_data AS target
          USING silver_ready_data AS source
          ON target.device_id=source.device_id
          AND target.event_ts=source.event_ts 

          WHEN MATCHED THEN UPDATE SET * 
          WHEN NOT MATCHED THEN INSERT *
          """)

# COMMAND ----------

print(f"Merged {clean_count} clean rows into silver.")

# COMMAND ----------

# Build the quarantine DataFrame with the columns we want to store
quarantine_ready_df=(
    quarantine_rows_df
    .withColumn("quarantine_time",current_timestamp())
    .withColumnRenamed("ingestion_time","bronze_ingest")
    .select(
        "device_id",
        "timestamp",
        "temperature",
        "humidity",
        "device_status",
        "location",
        "sensor_type",
        "source_file",
        "bronze_ingest",
        "rejection_reason",
        "quarantine_time"
    )
)

# COMMAND ----------

# Append the bad rows to the quarantine table (we keep ALL bad rows — never overwrite)
if quarantine_count>0:
    (
        quarantine_ready_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("iot_sensors.silver.silver_quarantine")
    )
    print(f"Sent {quarantine_count} bad rows to quarantine.")
else:
    print("No bad rows to quarantine.")

# COMMAND ----------

# Count rows in each layer for the final report
total_bronze=spark.table("iot_sensors.bronze.bronze_iot_data").count()
total_silver=spark.table("iot_sensors.silver.silver_iot_data").count()
total_quarantine=spark.table("iot_sensors.silver.silver_quarantine").count()
rows_added_to_silver=total_silver-row_count_before_processing

# COMMAND ----------

# Calculate what percentage of incoming Bronze rows passed quality checks
quality_pass_rate=((clean_count/new_row_count)*100) if new_row_count>0 else 0

# COMMAND ----------

print("=== Data Quality Report ===")
print(f" Bronze rows (total) : {total_bronze}")
print(f" New rows processed this run : {new_row_count}")
print(f" Passed quality checks : {clean_count}")
print(f" Failed quality checks : {quarantine_count}")
print(f" Quality Pass Rate : {quality_pass_rate:.2f}%")
print(f" Silver rows added this run : {rows_added_to_silver}")
print(f" Total silver rows now : {total_silver}")
print(f" Total quarantine rows now : {total_quarantine}")

# COMMAND ----------

# Show a breakdown of WHY rows were rejected.
print("Rejection Reason Breakdown :")
spark.table("iot_sensors.silver.silver_quarantine").groupBy("rejection_reason").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# Show a sample of clean Silver rows to confirm the transformation looks correct
print("Sample of clean silver rows (first 20) :")
spark.table("iot_sensors.silver.silver_iot_data").orderBy("device_id","event_ts").limit(20).show()

# COMMAND ----------

# Show which devices triggered the most alert events (temperature > 50 °C)

# COMMAND ----------

alert_row_count=spark.table("iot_sensors.silver.silver_iot_data").filter(col("is_alert")==True).count()
print(f"Total alert events across all devices : {alert_row_count :,}")

# COMMAND ----------

print("Alert counts per device : ")
spark.table("iot_sensors.silver.silver_iot_data").filter(col("is_alert")==True).groupBy("device_id").agg(count("*").alias("alert_count")).orderBy(col("alert_count").desc()).show()

# COMMAND ----------

