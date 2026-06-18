from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window

@dp.table(name="last_24_hour_weather")
def last_24_hour_weather():
    silver_current = spark.read.table("weather.forecast_and_current_weather.silver_current_weather")
    measurement_points = spark.read.table("weather.forecast_and_current_weather.openmeteo_measurement_points")
    
    cutoff_time = F.expr("current_timestamp() - interval 24 hours")
    
    return (
        silver_current.alias("l")
        .join(
            measurement_points.alias("r"),
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


@dp.table(name="last_weather")
def last_weather():
    df = spark.read.table("last_24_hour_weather")
    
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
