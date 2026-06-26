# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Configuraciones

# COMMAND ----------

# MAGIC %md
# MAGIC # Creando un catalogo
# MAGIC
# MAGIC ### ¿Qué es un catálogo?
# MAGIC
# MAGIC Un **catálogo** en bases de datos es una colección lógica que organiza y agrupa esquemas, tablas y otros objetos de datos. Sirve como un contenedor principal para gestionar el acceso, la seguridad y la organización de los datos dentro de una plataforma como Databricks o sistemas de bases de datos modernos.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Creando el Schema de trabajo

# COMMAND ----------

# MAGIC %md
# MAGIC # Funciones

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

def read_stream(spark, source_folder, schema):
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
        .option('cloudFiles.cleanSource','DELETE')
        .option('cloudFiles.cleanSource.retentionDuration',"7 days")
        .schema(schema)
        .load(source_folder)\
        .withColumns({"metadata_ingestion_timestamp":  F.from_utc_timestamp(F.current_timestamp(), "America/Panama"),
                      'metadata_file_name': F.col('_metadata.file_path')})
    )


# COMMAND ----------

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
            "hour": F.date_format(timestamp_col, "HH").cast('int'),
            "query_timestamp": F.to_timestamp(
                F.col("query_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"
            ),
        }
    )


# COMMAND ----------

# MAGIC %md 
# MAGIC # Slowly Changing Dimension (SCD)
# MAGIC
# MAGIC Existen varios tipos de SCD:
# MAGIC
# MAGIC - **SCD Type 0:** No se permite ningún cambio, los datos permanecen como fueron insertados.
# MAGIC - **SCD Type 1:** Se sobrescriben los datos antiguos con los nuevos, sin mantener historial.
# MAGIC - **SCD Type 2:** Se mantiene el historial completo de los cambios, agregando nuevas filas para cada cambio.
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## SCD Type 0  Append Only (Current):
# MAGIC
# MAGIC - Solo necesitas el snapshot del momento (como tu API del clima actual)
# MAGIC - Cada registro representa un evento único en el tiempo
# MAGIC - No hay "correcciones" o actualizaciones de datos pasados
# MAGIC
# MAGIC Esta api genera una captura de las condiciones del clima al momento de ejecutar el api, para esta tabla podemos guardar en bronze todo el historial tal cual viene de la fuente.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1
# MAGIC
# MAGIC - Se sobrescriben los datos antiguos con los nuevos, sin mantener historial.
# MAGIC - Solo se conserva la versión más reciente de cada registro.
# MAGIC - Ejemplo: Si las condiciones del clima cambian, el registro anterior se actualiza y no se guarda el valor anterior.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SCD type 2
# MAGIC
# MAGIC - Necesitas mantener el historial completo de cambios
# MAGIC - Quieres saber qué valor tenía un campo en un momento específico
# MAGIC - Los datos pueden cambiar y necesitas auditoría temporal
# MAGIC - Ejemplo en tu caso: si las condiciones del clima se actualizan/corrigen retroactivamente

# COMMAND ----------

import json
from pyspark.sql import Row
import random
from datetime import datetime, timedelta
import hashlib
import uuid
import json

from zoneinfo import ZoneInfo

def generate_synthetic_weather_data_json(folder,num_records=100):
    base_time = datetime.now(ZoneInfo("America/Panama"))
    data = []
    for i in range(num_records):
        record = {
            "latitude": 9.033391952514648,
            "longitude": -79.4896240234375,
            "timestamp": str(int((base_time + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0).timestamp())),
            "query_timestamp": (base_time + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "precipitation_probability": random.randint(0, 100),
            "precipitation": round(random.uniform(0, 50), 2),
            "weather_code": random.choice([0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99]),
            "wind_direction_10m": random.randint(0, 360),
            "wind_gusts_10m": round(random.uniform(0, 30), 2),
            "wind_speed_10m": round(random.uniform(0, 20), 2)
        }
        data.append(record)
    json_payload: str = json.dumps(
    data,
    ensure_ascii=False,
    default=str
    )

    content_hash: str = hashlib.sha256(
    json_payload.encode("utf-8")
    ).hexdigest()[:16]

    epoch_ns: int = time.time_ns()
    unique_id: str = uuid.uuid4().hex[:12]

    file_name: str = f'{epoch_ns}_{content_hash}_{unique_id}.json'
    file_path: str = folder + file_name

    print( file_path )

    # Ensure the directory exists before writing
    dbutils.fs.mkdirs(folder)

    # overwrite=False evita sobrescritura accidental
    dbutils.fs.put(file_path, json_payload, False)


output_path = "/Workspace/Users/jaquesada92@outlook.com/openmeteo-databricks-pipeline/taller/data/hourly/"
generate_synthetic_weather_data_json(output_path,3)

# COMMAND ----------

_sdf = spark.read.json(output_path)
schema = _sdf.schema

# COMMAND ----------

_sdf.count()

# COMMAND ----------

_sdf.groupBy('latitude','longitude','timestamp').agg(F.countDistinct('precipitation_probability'),F.countDistinct('precipitation'),F.countDistinct('weather_code'),F.countDistinct('wind_direction_10m'),F.countDistinct('wind_gusts_10m'),F.countDistinct('wind_speed_10m'),F.count('*')).display()

# COMMAND ----------

from scd2_utils import SCD2

sdf = (spark.readStream.schema(schema)
    .json(
       output_path
    )
    .withColumns({"file_name": F.col("_metadata.file_path"),
                  'query_timestamp': F.from_utc_timestamp(F.col("query_timestamp"), "America/Panama"),
                  'ingestion_timestamp':   F.from_utc_timestamp(F.current_timestamp(), "America/Panama")})
    .transform(parse_timestamp_column))

scd2_process = SCD2(sdf,'test.weather.scd2',['latitude','longitude','timestamp'],
                      tracking_cols=['precipitation_probability','precipitation','weather_code','wind_direction_10m','wind_gusts_10m','wind_speed_10m'],
                      sequence_by='query_timestamp')


scd2_process.stream_update_bronze('/Workspace/Users/jaquesada92@outlook.com/openmeteo-databricks-pipeline/taller/checkpoint/hourly').awaitTermination() 

# COMMAND ----------

_sdf.distinct().display()

# COMMAND ----------

# MAGIC %sql select * from test.weather.scd2 

# COMMAND ----------

_sdf.display()

# COMMAND ----------


