from pyspark import pipelines as dp
import pyspark.sql.functions as F

username = spark.conf.get("username")
mode = spark.conf.get("mode")
schema = "forecast_and_current_weather"
bronze_folder = (
    f"/Workspace/Users/{username}/openmeteo-databricks-pipeline/data/forecast/{mode}"
)
table_name = f"{mode}_forecast_weather" if mode != "current" else f"{mode}_weather"
bronze_table = f"bronze_{table_name}"


@dp.table(name=bronze_table)
def bronze_weather():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(bronze_folder)
        .withColumn(   "metadata_ingestion_timestamp", F.current_timestamp() )
    )


@dp.table(
    name=f"silver_{table_name}"
)
def silver_weather():
    
    # Leer tablas de referencia con broadcast para joins eficientes
    weather_code = F.broadcast(spark.read.table("weather_codes"))
    src_table = spark.readStream.table(bronze_table)
    timestamp_col = F.from_unixtime(F.col("timestamp"))

    return (
        src_table.select(
            [
                F.round(F.col("latitude").cast("double"), 6).alias("latitude"),
                F.round(F.col("longitude").cast("double"), 6).alias("longitude"),
                F.round(F.col("elevation").cast("double"), 2).alias("elevation"),
                F.to_timestamp(
                    F.col("query_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"
                ).alias("query_timestamp"),
                timestamp_col.cast("timestamp").alias("timestamp"),
                F.to_date(timestamp_col).alias("date"),
                F.date_format(timestamp_col, "HH:mm:ss").alias("time"),
                F.col("timezone"),
                F.col("weather_code").cast("int").alias("weather_code"),
                F.round(F.col("precipitation").cast("double"), 2).alias(
                    "precipitation"
                ),
                F.round(F.col("rain").cast("double"), 2).alias("rain"),
                F.round(F.col("temperature_2m").cast("double"), 2).alias(
                    "temperature_2m"
                ),
                F.round(F.col("relative_humidity_2m").cast("double"), 2).alias(
                    "relative_humidity_2m"
                ),
                F.round(F.col("wind_direction_10m").cast("double"), 2).alias(
                    "wind_direction_10m"
                ),
                F.expr("weather.forecast_and_current_weather.get_wind_direction(wind_direction_10m)").alias(
                    "wind_direction_description"
                ),
                F.round(F.col("wind_gusts_10m").cast("double"), 2).alias(
                    "wind_gusts_10m"
                ),
                F.round(F.col("wind_speed_10m").cast("double"), 2).alias(
                    "wind_speed_10m"
                ),
            ]
        )
        .join(weather_code, on="weather_code", how="left")
        .select(
            [
                "latitude",
                "longitude",
                "elevation",
                "query_timestamp",
                "timestamp",
                "date",
                "time",
                "timezone",
                "weather_code",
                "weather_category",
                "weather_intensity",
                "precipitation",
                "rain",
                "temperature_2m",
                "relative_humidity_2m",
                "wind_direction_description",
                "wind_direction_10m",
                "wind_gusts_10m",
                "wind_speed_10m",
            ]
        )
    )


