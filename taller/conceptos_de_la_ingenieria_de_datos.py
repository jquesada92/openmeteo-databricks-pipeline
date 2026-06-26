# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Escoginedo la Herramienta adecuada
# MAGIC ![](/Workspace/Users/jaquesada92@outlook.com/openmeteo-databricks-pipeline/taller/img/pandas_vs_spark.png)
# MAGIC
# MAGIC ## Pandas Vs Pyspark
# MAGIC
# MAGIC | Escenario | Mejor opción |
# MAGIC |---|---|
# MAGIC | Archivo pequeño CSV o Excel | Pandas |
# MAGIC | Análisis exploratorio rápido | Pandas |
# MAGIC | Prototipo local | Pandas |
# MAGIC | Pipeline productivo | PySpark |
# MAGIC | Grandes volúmenes de datos | PySpark |
# MAGIC | Procesamiento distribuido | PySpark |
# MAGIC | Lakehouse con Delta Lake | PySpark |
# MAGIC | Streaming con Kafka o Auto Loader | PySpark |
# MAGIC | SCD Type 2 / MERGE | PySpark |
# MAGIC | Procesamiento incremental | PySpark |

# COMMAND ----------

forecast_dir = "/Workspace/Users/jaquesada92@outlook.com/openmeteo-databricks-pipeline/taller/data/hourly/"
generate_synthetic_weather_data_json(forecast_dir,num_records=100)

# COMMAND ----------

sdf = spark.read.format('json').load(forecast_dir)
sdf.limit(5).display()

# COMMAND ----------

import pandas as pd
from glob import glob 
df = pd.concat(list(map(pd.read_json,glob(forecast_dir + '*.json'))))
display(df.head(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Ventaja de PySpark: Lazy Evaluation vs Pandas
# MAGIC
# MAGIC Una de las ventajas importantes de PySpark frente a Pandas es el uso de **lazy evaluation**.
# MAGIC
# MAGIC En Pandas, cuando se lee un archivo o se ejecuta una transformación, normalmente la operación se ejecuta de inmediato y los datos se cargan en la memoria de una sola máquina. Esto funciona muy bien con datasets pequeños o medianos, pero puede convertirse en un problema cuando el volumen de datos crece.
# MAGIC
# MAGIC En PySpark, muchas operaciones no se ejecutan inmediatamente. Spark primero construye un **plan lógico** con las transformaciones que se quieren aplicar. La ejecución real ocurre solamente cuando se llama una **acción**, como `show()`, `count()`, `write()`, `collect()` o `display()`.
# MAGIC
# MAGIC ### Comparación rápida
# MAGIC
# MAGIC | Aspecto | Pandas | PySpark |
# MAGIC |---|---|---|
# MAGIC | Ejecución | Inmediata | Lazy evaluation |
# MAGIC | Memoria | Carga datos en memoria local | Procesa datos de forma distribuida |
# MAGIC | Optimización | Limitada al proceso local | Spark optimiza el plan antes de ejecutar |
# MAGIC | Escalabilidad | Depende de una sola máquina | Puede usar múltiples nodos |
# MAGIC | Mejor para | Análisis pequeño o prototipos | Pipelines productivos y grandes volúmenes |
# MAGIC
# MAGIC
# MAGIC ### Tipos de Transformaciones:
# MAGIC
# MAGIC
# MAGIC | Tipo   | Genera Shuffle | Ejemplos                                 |
# MAGIC | ------ | -------------- | ---------------------------------------- |
# MAGIC | Narrow | No             | `select`, `filter`, `withColumn`         |
# MAGIC | Wide   | Sí             | `groupBy`, `join`, `distinct`, `orderBy` |
# MAGIC
# MAGIC El shuffle ocurre cuando Spark necesita mover datos entre particiones o executors para completar una operación.
# MAGIC
# MAGIC

# COMMAND ----------

import os
import gc
import psutil

def show_driver_memory(label: str = ""):
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024

    print(f"{label} | Driver memory used: {memory_mb:,.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Ingesta de datos
# MAGIC
# MAGIC ## ¿Qué es Structured Streaming?
# MAGIC
# MAGIC **Structured Streaming** es el motor de Apache Spark para procesar datos en tiempo real o de forma incremental usando la misma lógica de DataFrames.
# MAGIC
# MAGIC La idea principal es que Spark trata un flujo continuo de datos como si fuera una tabla que se actualiza constantemente.
# MAGIC
# MAGIC Por ejemplo, puedes leer datos nuevos desde:
# MAGIC
# MAGIC | Fuente | Ejemplo |
# MAGIC |---|---|
# MAGIC | Archivos nuevos | JSON, CSV, Parquet |
# MAGIC | Kafka | Eventos en tiempo real |
# MAGIC | Delta Lake | Tablas Delta incrementales |
# MAGIC | Auto Loader | Ingesta incremental en Databricks |
# MAGIC
# MAGIC

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
