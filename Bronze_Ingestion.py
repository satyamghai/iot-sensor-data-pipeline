# Databricks notebook source
spark.sql("""CREATE TABLE IF NOT EXISTS iot_sensors.bronze.bronze_iot_data(
    device_id STRING,
    timestamp STRING,
    temperature STRING,
    humidity STRING,
    device_status STRING,
    location STRING,
    sensor_type STRING,
    ingestion_time TIMESTAMP,
    source_file STRING
)
COMMENT 'Bronze raw IOT sensor data - no transformations applied'
""")

# COMMAND ----------

# Check the number of rows already in the bronze table
# It will be 0 on the first run 
row_count_before_ingestion=spark.table("iot_sensors.bronze.bronze_iot_data").count()

# COMMAND ----------

print(f"Rows in bronze before this run : {row_count_before_ingestion}")

# COMMAND ----------

# Read raw CSV files from the Volume with Auto Loader
raw_stream=(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaLocation","/Volumes/iot_sensors/default/rawdata/schema")
    .option("header","true")
    .option("inferSchema","false")
    .load("/Volumes/iot_sensors/default/rawdata/streaming/")
)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

bronze_stream=(
    raw_stream
    .withColumn("ingestion_time",current_timestamp())
    .withColumn("source_file",col("_metadata.file_path"))
)

# COMMAND ----------

bronze_query=(
    bronze_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation","/Volumes/iot_sensors/default/rawdata/checkpoint")
    .outputMode("append")
    .option("mergeSchema","true")
    .trigger(availableNow=True)
    .toTable("iot_sensors.bronze.bronze_iot_data")
)

# COMMAND ----------

# Check number of rows in bronze after ingestion
row_count_after_ingestion=spark.table("iot_sensors.bronze.bronze_iot_data").count()

# COMMAND ----------

# Calculate how many new rows were added during this run
row_added_this_run=row_count_after_ingestion-row_count_before_ingestion

# COMMAND ----------

print(f"Rows in bronze before this run : {row_count_before_ingestion}")
print(f"Rows added in this run : {row_added_this_run}")
print(f"Total rows in bronze now : {row_count_after_ingestion}")

# COMMAND ----------

bronze_df=spark.table("iot_sensors.bronze.bronze_iot_data")

# COMMAND ----------

print(f"Total rows in bronze : {bronze_df.count()}")

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# Files ingested so far
bronze_df.groupBy("source_file").agg(count("*").alias("Row COUNT"),min("ingestion_time").alias("First seen"),max("ingestion_time").alias("Last Seen")).orderBy("First seen").show()

# COMMAND ----------


# Device × Status breakdown
bronze_df.groupBy("device_id","device_status").count().orderBy("device_id","device_status").show()

# COMMAND ----------

