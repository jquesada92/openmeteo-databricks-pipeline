import pyspark.sql.functions as F


def create_bronze_weather_stream(spark, source_folder):
    """
    Create bronze layer streaming dataframe from cloudFiles

    Args:
        spark: SparkSession
        source_folder: Path to source JSON files

    Returns:
        Streaming DataFrame with raw weather data
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_folder)
        .withColumn("metadata_ingestion_timestamp", F.current_timestamp())
    )


def parse_timestamp_column(
    df, timestamp_col_name="timestamp", timezone="America/Panama"
):
    """
    Parse timestamp column handling both nanoseconds and seconds formats

    Args:
        df: Input DataFrame
        timestamp_col_name: Name of the timestamp column to parse

    Returns:
        DataFrame with epoch_seconds column added
    """
    df = df.withColumn(
        "epoch_seconds",
        F.when(
            F.length(timestamp_col_name) >= 18,  # nanosegundos
            F.col(timestamp_col_name).cast("double") / F.lit(1_000_000_000),
        ).otherwise(
            F.col(timestamp_col_name).cast("double")  # segundos
        ),
    )
    timestamp_col = F.from_utc_timestamp(
        F.from_unixtime(F.col("epoch_seconds")), timezone
    )
    return df.withColumns(
        {
            "timestamp": timestamp_col.cast("timestamp"),
            "date": F.to_date(timestamp_col),
            "hour": F.date_format(timestamp_col, "HH:mm:ss"),
            "query_timestamp": F.to_timestamp(
                F.col("query_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"
            ),
        }
    )


def transform_weather_columns(df):
    """
    Apply common transformations to weather data columns

    Args:
        df: Input DataFrame with parsed timestamp
        timezone: Target timezone for timestamp conversion

    Returns:
        DataFrame with standardized weather columns
    """
    double_cols = [
        "latitude",
        "longitude",
        "elevation",
        "precipitation",
        "rain",
        "temperature",
        "relative_humidity",
        "wind_direction",
        "wind_gusts",
        "wind_speed",
    ]
    columns = df.columns
    selected_columns = list(
        filter(lambda x: any(x if col in x else None for col in double_cols), columns)
    )
    wind_direction_cols = list(filter(lambda x: "wind_direction" in x, columns))

    return df.withColumns(
        {
            **{x: F.col(x).cast("double") for x in selected_columns},
            **{
                f"description_{x}": F.expr(
                    f"weather.forecast_and_current_weather.get_wind_direction({x})"
                )
                for x in wind_direction_cols
            },
        }
    )


def enrich_with_weather_codes(spark, df):
    """
    Join weather data with weather code reference table

    Args:
    df: Input DataFrame with weather_code column
    spark: SparkSession

    Returns:
    DataFrame enriched with weather category and intensity
    """
    weather_code = F.broadcast(
        spark.read.table("weather_codes").withColumn(
            "weather_code", F.col("weather_code").cast("int")
        )
    )
    return df.withColumn("weather_code", F.col("weather_code").cast("int")).join(
        weather_code, on="weather_code", how="left"
    )
