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
schema = "forecast_and_current_weather"
bronze_folder = f"/Workspace/Users/{username}/openmeteo-databricks-pipeline/data/forecast/hourly"



@dp.table
def raw_hourly_forecast_weather():
    """Bronze layer: Raw ingestion from hourly forecast JSON files"""
    return create_bronze_weather_stream(spark, bronze_folder)



dp.create_streaming_table(
    name="bronze_hourly_forecast_weather_scd_type2",
    comment="Employee records with full change history (SCD Type 2). Tracks changes in salary, allowance, and start date.",
    # ✅ LIQUID CLUSTERING para tabla SCD
    # Optimiza queries por: empleado (keys) + rango de fechas
    cluster_by=["latitude",'longitude'],
)

# Create Auto CDC flow to track changes
dp.create_auto_cdc_flow(
    target="bronze_hourly_forecast_weather_scd_type2",
    source="raw_hourly_forecast_weather",
    keys=["latitude",'longitude','timestamp'],  # Primary keys for row identification
    sequence_by="query_timestamp",  # Column for ordering events (timestamp)
    stored_as_scd_type=2,  # Enable SCD Type 2 - adds __START_AT and __END_AT columns
    track_history_column_list = ['weather_code',
                                'precipitation',
                                'rain',
                                'showers',
                            'temperature_120m',
                            'temperature_180m',
                            'temperature_2m',
                            'temperature_80m',
                            'wind_direction_10m',
                            'wind_direction_120m',
                            'wind_direction_180m',
                            'wind_direction_80m',]
)


@dp.table
def silver_hourly_forecast_weather():
    """Silver layer: Parse timestamp, rename columns, and enrich with weather codes"""
    return enrich_with_weather_codes(spark,
        dp.read("bronze_hourly_forecast_weather_scd_type2")
        .transform(parse_timestamp_column)\
            .transform(transform_weather_columns)
            ).select(
                [
                'latitude',
                'longitude',
                'elevation',
                'date',
                'hour',
                'query_timestamp',
                'timestamp',
                'timezone',
                'precipitation',
                'pressure_msl',
                'rain',
                'relative_humidity_2m',
                'showers',
                'temperature_120m',
                'temperature_180m',
                'temperature_2m',
                'temperature_80m',
                'weather_code',
                'wind_speed_10m',
                'wind_gusts_10m',
                'wind_direction_10m',
                'description_wind_direction_10m',
                'wind_speed_80m',
                'wind_direction_80m',
                'description_wind_direction_80m',
                'wind_speed_120m',
                'wind_direction_120m',
                'description_wind_direction_120m',
                'wind_speed_180m',
                'wind_direction_180m',
                'description_wind_direction_180m',
                'metadata_ingestion_timestamp']
            )


@dp.table
def next_24_and_past_12_weather():
    silver_current = dp.read("silver_hourly_forecast_weather")
    sites = spark.read.table("locations")

    
    return (
        silver_current
        .filter((F.col("timestamp") >= F.expr("current_timestamp() - interval 12 hours") )&
                 (F.col("timestamp") <= F.expr("current_timestamp() + interval 24 hours" ))
        )
        .alias("l")
        .join(
            sites.alias("r"),
            (F.col("l.latitude") == F.col("r.latitude")) & 
            (F.col("l.longitude") == F.col("r.longitude")),
            "left"
        )
       
        .select(
            F.col("r.country"),
            F.col("r.province"),
            F.col("r.place_name"),
            F.col("r.city"),
            F.col("l.*")
        )
    )



