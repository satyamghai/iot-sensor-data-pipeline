# Databricks notebook source
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS iot_sensors")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS iot_sensors.bronze")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS iot_sensors.silver")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS iot_sensors.gold")

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS iot_sensors.default.rawdata")

# COMMAND ----------

