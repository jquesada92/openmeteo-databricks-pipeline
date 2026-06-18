"""
Este script actualiza la tabla de puntos de medición (`openmeteo_measurement_points`) con información de nombre y dirección de lugares,
utilizando resultados de APIs externas. El proceso consiste en identificar coordenadas (latitud, longitud) que no tienen información de país,
consultar una API para obtener el nombre y dirección del lugar más cercano, y guardar estos datos en la tabla de medición.
Esto es necesario porque al hacer join con las tablas de locations, las coordenadas no coinciden exactamente y no se puede obtener el nombre del lugar directamente.
"""

import pandas as pd
from utils import get_place_name

schema = 'weather.forecast_and_current_weather'
sink_table_name = f'{schema}.openmeteo_measurement_points'
current_weather_table_name = f'{schema}.silver_current_weather'
forecast_table_name = f'{schema}.silver_hourly_forecast_weather'
sink_df = spark.read.table(sink_table_name)

# Extrae coordenadas únicas de la tabla de clima actual
df = (
    spark.read.table(current_weather_table_name)
    .select('latitude', 'longitude')
    .distinct()
)

# Si existe la tabla de pronóstico, agrega sus coordenadas únicas
if spark.catalog.tableExists(forecast_table_name):
    df = (
        df.unionByName(
            spark.read.table(forecast_table_name)
            .select('latitude', 'longitude')
            .distinct()
        )
        .distinct()
    )

# Identifica coordenadas que no tienen información de país en la tabla de medición
updates_df = df.join(
    sink_df.where('country is null'),
    on=['latitude', 'longitude'],
    how='left_anti'
)

# Si hay coordenadas sin información de país, consulta la API para obtener nombre y dirección
if not updates_df.isEmpty():
    print('updates')
    data = updates_df.toPandas()[['latitude', 'longitude']].apply(
        lambda row: get_place_name(row['latitude'], row['longitude']), axis=1
    )
    spark.createDataFrame(pd.json_normalize(data)).write.mode('append').saveAsTable(sink_table_name)
    update = 1
else:
    print('no updates')
    update = 0

# Guarda el resultado del proceso para uso en otros tasks
dbutils.jobs.taskValues.set(key="updates", value=update)