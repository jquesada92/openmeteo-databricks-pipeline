# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Docuemtacion

# COMMAND ----------

# MAGIC %pip install openmeteo-requests;
# MAGIC %pip install requests-cache retry-requests numpy pandas

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE CATALOG boquete;
# MAGIC USE CATALOG boquete;
# MAGIC CREATE SCHEMA clima;

# COMMAND ----------

# MAGIC %sql select * from weather.open_meteo.bronze_current_weather

# COMMAND ----------

# MAGIC %sql select * from table_changes('weather.open_meteo.bronze_current_weather',339,341)

# COMMAND ----------

RESTORE TABLE weather.open_meteo.bronze_current_weather TO VERSION AS OF 339

# COMMAND ----------

# MAGIC %sql select count(*) from weather.open_meteo.bronze_current_weather timestamp as of '2026-07-03T23:10:04.000+00:00'

# COMMAND ----------

# MAGIC %sql select count(*) from weather.open_meteo.bronze_current_weather version as of 340

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY weather.open_meteo.bronze_current_weather

# COMMAND ----------



# COMMAND ----------


