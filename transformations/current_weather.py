from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from transformations_common import (
    create_bronze_weather_stream,
    parse_timestamp_column,
    transform_weather_columns,
    enrich_with_weather_codes
)

# Configuration
username = spark.conf.get("username")
bronze_folder = f"/Workspace/Users/{username}/openmeteo-databricks-pipeline/data/forecast/current"


@dp.table
def bronze_current_weather():
    """Bronze layer: Raw ingestion from JSON files"""
    return create_bronze_weather_stream(spark, bronze_folder)


@dp.table
def silver_current_weather():
    """Silver layer: Cleaned and transformed current weather data"""
    # Read from bronze
    src_table = spark.readStream.table("bronze_current_weather")
    
    # Parse timestamp
    src_table = parse_timestamp_column(src_table)
    
    # Transform columns
    transformed = transform_weather_columns(src_table)
    
    # Enrich with weather codes
    return enrich_with_weather_codes(spark,transformed)



@dp.table
def last_8_hour_weather():
    silver_current = dp.read("silver_current_weather")
    sites = spark.read.table("locations")
    
    cutoff_time = F.expr("current_timestamp() - interval 8 hours")
    
    return (
        silver_current.alias("l")
        .join(
            sites.alias("r"),
            (F.col("l.latitude") == F.col("r.latitude")) & 
            (F.col("l.longitude") == F.col("r.longitude")),
            "left"
        )
        .filter(F.col("l.timestamp") >= cutoff_time)
        .select(
            F.col("r.country"),
            F.col("r.province"),
            F.col("r.place_name"),
            F.col("r.city"),
            F.col("l.*")
        )
    )


@dp.table
def last_weather():
    df = dp.read("last_8_hour_weather")
    return (
        df
        .withColumn(
            "max_timestamp",
            F.max("timestamp").over(
            Window.partitionBy("latitude", "longitude")
            )
        )
        .filter(F.col("max_timestamp") == F.col("timestamp"))
        .drop("max_timestamp")
    )
