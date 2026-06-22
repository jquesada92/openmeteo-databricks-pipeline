from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from transformations_common import (
    create_bronze_weather_stream,
    parse_timestamp_column,
    transform_weather_columns,
    enrich_with_weather_codes,
)

# Configuration
username = spark.conf.get("username")
bronze_folder = (
    f"/Workspace/Users/{username}/openmeteo-databricks-pipeline/data/forecast/current"
)


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
    return enrich_with_weather_codes(spark, transformed)


@dp.table
def gold_current_and_forecast_weather():

    def filter_timestamp(df):
        return df.withColumn(
            "forecast_horizon_hours",
            F.timestamp_diff(
                "HOUR",
                F.from_utc_timestamp(F.current_timestamp(), "America/Panama"),
                F.col("timestamp")
              
            )
        ).filter(F.abs(F.col("forecast_horizon_hours")) <= 4)

    forecast_df = (
        dp.read("silver_hourly_forecast_weather")
        .withColumn("label", F.lit("Forecast"))
        .transform(filter_timestamp)
    )

    actual_df = (
        dp.read("silver_current_weather")
        .drop("is_day", "timezone_abbreviation", "epoch_seconds", "_rescued_data")
        .transform(filter_timestamp)
        .withColumns(
            {
                "label": F.lit("Actual"),
                "max_timestamp": F.max("timestamp").over(
                    Window.partitionBy("latitude", "longitude")
                ),
                "label": F.lit("Actual"),
                "last_measure": F.when(
                    F.col("timestamp") == F.col("max_timestamp"), F.lit(True)
                ).otherwise(F.lit(False)),
            }
        )
    )

    sites = spark.read.table("locations")

    return (
        forecast_df.unionByName(actual_df, allowMissingColumns=True)
        .alias("l")
        .join(
            sites.alias("r"),
            (F.col("l.latitude") == F.col("r.latitude"))
            & (F.col("l.longitude") == F.col("r.longitude")),
            "left",
        )
        .select(
            F.col("r.country"),
            F.col("r.province"),
            F.col("r.place_name"),
            F.col("r.city"),
            F.col("l.*"),
        )
    )
