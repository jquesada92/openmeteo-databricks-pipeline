# Open-Meteo Databricks Pipeline

Pipeline de datos meteorológicos en tiempo real usando Databricks Lakeflow Spark Declarative Pipelines (SDP) con arquitectura Medallion para procesar datos de pronósticos del clima desde la API Open-Meteo.

## 📋 Descripción General

Este proyecto implementa un pipeline ETL completo que:
* Ingiere datos de pronósticos meteorológicos desde la API Open-Meteo
* Procesa datos usando arquitectura Medallion (Bronze → Silver → Gold)
* Implementa Change Data Capture (CDC) con SCD Type 2 para rastrear cambios históricos
* Proporciona datos limpios y enriquecidos para análisis y visualización

## 🏗️ Arquitectura de Datos y Lineage

### Pipeline de Pronóstico Horario (Forecast)

```
📁 Archivos JSON (/data/forecast/hourly/*.json)
        ↓ [Auto Loader - cloudFiles]
        
🟤 BRONZE LAYER
├─ raw_hourly_forecast_weather (Streaming Table)
│  └─ Ingesta cruda con Auto Loader
│  └─ Schema evolution habilitado
│  
       ↓ [Auto CDC Flow]
       
├─ bronze_hourly_forecast_weather_scd_type2 (Streaming Table - SCD Type 2)
│  └─ Rastreo de cambios históricos
│  └─ Keys: [latitude, longitude, timestamp]
│  └─ Sequence by: query_timestamp
│  └─ Liquid Clustering: [latitude, longitude]
│  └─ Columnas rastreadas: weather_code, precipitation, rain, temperatures, wind_direction
│  
       ↓ [Transformaciones + Enriquecimiento]
       
⚪ SILVER LAYER
├─ silver_hourly_forecast_weather (Materialized View)
│  └─ Timestamps parseados (date, hour)
│  └─ Columnas de clima transformadas
│  └─ Enriquecido con weather_codes
│  └─ Descripciones de dirección de viento
│  
       ↓ [Filtros temporales + Join con locations]
       
🟡 GOLD LAYER
└─ next_24_and_past_12_weather (Materialized View)
   └─ Últimas 12 horas + próximas 24 horas
   └─ Join con tabla de ubicaciones
   └─ Información geográfica enriquecida (país, provincia, ciudad)
```

### Pipeline de Clima Actual (Current Weather)

```
📁 Archivos JSON (/data/forecast/current/*.json)
        ↓ [Auto Loader]
        
🟤 BRONZE LAYER
├─ bronze_current_weather (Streaming Table)
│  
       ↓ [Parse + Transform + Enrich]
       
⚪ SILVER LAYER
├─ silver_current_weather (Streaming Table)
│  
       ↓ [Filter últimas 8 horas + Join locations]
       
🟡 GOLD LAYER
├─ last_8_hour_weather (Materialized View)
│  
       ↓ [Window function - max timestamp]
       
└─ last_weather (Materialized View)
   └─ Última medición por ubicación
```

### Tablas de Referencia

```
📋 REFERENCE TABLES
├─ locations
│  └─ Información geográfica (país, provincia, ciudad, coordenadas)
│  └─ Usado en joins de capas Gold
│
└─ weather_codes
   └─ Diccionario de códigos de clima
   └─ Categorías e intensidades
   └─ Broadcast join en Silver layer
```

## 📊 Datasets y Schemas

### Catálogo: `weather`
#### Schema: `forecast_and_current_weather`

| Dataset | Tipo | Layer | Filas | Descripción |
|---------|------|-------|-------|-------------|
| `raw_hourly_forecast_weather` | Streaming Table | Bronze | 33,408 | Ingesta cruda de pronósticos horarios |
| `bronze_hourly_forecast_weather_scd_type2` | Streaming Table (SCD2) | Bronze | - | Histórico de cambios en pronósticos |
| `silver_hourly_forecast_weather` | Materialized View | Silver | 32,118 | Pronósticos transformados y enriquecidos |
| `next_24_and_past_12_weather` | Materialized View | Gold | - | Ventana temporal de 36 horas |
| `bronze_current_weather` | Streaming Table | Bronze | - | Ingesta cruda de clima actual |
| `silver_current_weather` | Streaming Table | Silver | - | Clima actual transformado |
| `last_8_hour_weather` | Materialized View | Gold | - | Últimas 8 horas de mediciones |
| `last_weather` | Materialized View | Gold | - | Última medición por ubicación |
| `locations` | Table | Reference | - | Catálogo de ubicaciones |
| `weather_codes` | Table | Reference | - | Códigos de clima |

### Columnas Clave

**Coordenadas y Ubicación:**
* `latitude`, `longitude`, `elevation`
* `country`, `province`, `city`, `place_name`
* `timezone`, `timezone_abbreviation`

**Temporal:**
* `timestamp` - Timestamp completo
* `date` - Fecha (DATE)
* `hour` - Hora (HH:mm:ss)
* `query_timestamp` - Momento de la consulta API
* `__START_AT`, `__END_AT` - Validez temporal (solo SCD Type 2)

**Variables Meteorológicas:**
* `temperature_2m`, `temperature_80m`, `temperature_120m`, `temperature_180m`
* `precipitation`, `rain`, `showers`
* `relative_humidity_2m`
* `pressure_msl` - Presión al nivel del mar
* `weather_code` - Código WMO del clima
* `wind_speed_10m`, `wind_speed_80m`, `wind_speed_120m`, `wind_speed_180m`
* `wind_direction_10m`, `wind_direction_80m`, `wind_direction_120m`, `wind_direction_180m`
* `description_wind_direction_*` - Descripciones textuales (N, NE, E, SE, etc.)
* `wind_gusts_10m`

**Metadatos:**
* `metadata_ingestion_timestamp` - Timestamp de ingesta en pipeline

## 📂 Estructura del Proyecto

```
openmeteo-databricks-pipeline/
├── README.md                                   # Este archivo
├── .gitignore                                  # Archivos excluidos de Git
│
├── data/                                       # Datos de origen
│   ├── locations.csv                           # Catálogo de ubicaciones
│   └── forecast/
│       ├── current/                            # JSON - Clima actual
│       └── hourly/                             # JSON - Pronósticos horarios
│
├── scripts/                                    # Scripts de ingesta
│   ├── open_meteo_weather_ingestion.py         # Cliente API Open-Meteo
│   └── utils.py                                # Funciones auxiliares
│
└── transformations/                            # Pipeline SDP
    ├── forecast.py                             # Pipeline de pronósticos
    ├── current_weather.py                      # Pipeline de clima actual
    └── transformations_common.py               # Funciones compartidas
```

## 🔧 Tecnologías y Componentes

### Databricks Features
* **Lakeflow Spark Declarative Pipelines (SDP)** - Framework ETL declarativo
* **Auto Loader (cloudFiles)** - Ingesta incremental de archivos
* **Auto CDC Flow** - Change Data Capture con SCD Type 2
* **Liquid Clustering** - Optimización de queries
* **Unity Catalog** - Gobierno de datos

### Procesamiento
* **PySpark** - Transformaciones distribuidas
* **Streaming Tables** - Procesamiento continuo
* **Materialized Views** - Vistas optimizadas con refresh incremental

### API y Datos
* **Open-Meteo API** - Proveedor de datos meteorológicos
* **requests-cache** - Cache de respuestas API (1 hora)
* **retry-requests** - Reintentos automáticos con backoff exponencial

## ⚙️ Configuración del Pipeline

### Pipeline: `dlt_forecast_hourly_weather`

**Settings:**
* **Pipeline Type:** Workspace
* **Target:** `weather.forecast_and_current_weather`
* **Root Path:** `/Workspace/Users/{username}/openmeteo-databricks-pipeline/transformations`
* **Source Code:** `transformations/forecast.py`
* **Serverless:** ✅ Enabled
* **Photon:** ✅ Enabled
* **Channel:** CURRENT
* **Development:** ❌ (Production mode)

**Configuration Parameters:**
```python
{
  "username": "jaquesada92@outlook.com",
  "mode": "hourly"
}
```

### Características SCD Type 2

El dataset `bronze_hourly_forecast_weather_scd_type2` implementa:

* **Primary Keys:** `[latitude, longitude, timestamp]`
* **Sequence Column:** `query_timestamp` - Ordena eventos
* **Tracked Columns:** Columnas cuyos cambios se rastrean:
  * `weather_code`, `precipitation`, `rain`, `showers`
  * `temperature_2m`, `temperature_80m`, `temperature_120m`, `temperature_180m`
  * `wind_direction_10m`, `wind_direction_80m`, `wind_direction_120m`, `wind_direction_180m`

**Columnas Generadas por SCD Type 2:**
* `__START_AT` - Inicio de validez del registro
* `__END_AT` - Fin de validez (NULL = registro actual)

## 🚀 Uso

### 1. Configuración Inicial

```sql
-- Crear catálogo si no existe
CREATE CATALOG IF NOT EXISTS weather;

-- Crear schema
CREATE SCHEMA IF NOT EXISTS weather.forecast_and_current_weather;

-- Cargar tabla de ubicaciones
-- (Importar desde data/locations.csv)
```

### 2. Ejecutar Ingesta de Datos

```python
from scripts.open_meteo_weather_ingestion import OpenMeteoWeatherIngestion

# Inicializar cliente
ingestion = OpenMeteoWeatherIngestion(
    username="tu_usuario@ejemplo.com",
    dbutils=dbutils
)

# Obtener pronósticos horarios
ingestion.get_weather(
    latitude=[8.99, 9.36],  # Panamá
    longitude=[-79.52, -79.89],
    mode='hourly'
)

# Obtener clima actual
ingestion.get_weather(
    latitude=8.99,
    longitude=-79.52,
    mode='current'
)
```

### 3. Ejecutar Pipeline

```python
# Desde la UI de Databricks:
# Workflows → Pipelines → dlt_forecast_hourly_weather → Start

# O programáticamente:
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.pipelines.start_update(
    pipeline_id="22324c07-f417-4e26-b944-994a7f2e9d77",
    full_refresh=False
)
```

### 4. Consultar Datos

```sql
-- Pronóstico para las próximas 24 horas
SELECT 
    country,
    city,
    timestamp,
    temperature_2m,
    precipitation,
    weather_code,
    wind_speed_10m,
    description_wind_direction_10m
FROM weather.forecast_and_current_weather.next_24_and_past_12_weather
WHERE timestamp >= current_timestamp()
ORDER BY timestamp;

-- Última medición por ubicación
SELECT 
    country,
    city,
    timestamp,
    temperature_2m,
    weather_code
FROM weather.forecast_and_current_weather.last_weather
ORDER BY country, city;

-- Consultar histórico de cambios (SCD Type 2)
SELECT 
    latitude,
    longitude,
    timestamp,
    temperature_2m,
    __START_AT,
    __END_AT
FROM weather.forecast_and_current_weather.bronze_hourly_forecast_weather_scd_type2
WHERE latitude = 8.99 
  AND longitude = -79.52
  AND __END_AT IS NOT NULL  -- Solo registros históricos
ORDER BY __START_AT DESC;
```

## 📈 Funciones Comunes (transformations_common.py)

### `create_bronze_weather_stream(spark, source_folder)`
Crea streaming dataframe con Auto Loader desde archivos JSON.

**Features:**
* Schema evolution automático
* Timestamp de ingesta
* Formato: cloudFiles JSON

### `parse_timestamp_column(df, timestamp_col_name, timezone)`
Parsea timestamps Unix (segundos o nanosegundos) a datetime.

**Transforma:**
* `timestamp` → TIMESTAMP
* `date` → DATE
* `hour` → STRING (HH:mm:ss)
* `query_timestamp` → TIMESTAMP

### `transform_weather_columns(df)`
Estandariza columnas meteorológicas.

**Aplica:**
* Cast a DOUBLE de variables numéricas
* Genera descripciones de dirección de viento usando UDF

### `enrich_with_weather_codes(spark, df)`
Enriquece con tabla de referencia de códigos de clima.

**Join:**
* Broadcast join con `weather_codes`
* Agrega categorías e intensidades

## 🔍 Queries de Ejemplo

### Análisis Temporal

```sql
-- Evolución de temperatura en las próximas 24 horas
SELECT 
    city,
    timestamp,
    temperature_2m,
    LAG(temperature_2m) OVER (
        PARTITION BY latitude, longitude 
        ORDER BY timestamp
    ) as prev_temp,
    temperature_2m - LAG(temperature_2m) OVER (
        PARTITION BY latitude, longitude 
        ORDER BY timestamp
    ) as temp_change
FROM weather.forecast_and_current_weather.next_24_and_past_12_weather
WHERE timestamp >= current_timestamp()
ORDER BY city, timestamp;
```

### Agregaciones

```sql
-- Resumen diario por ciudad
SELECT 
    country,
    city,
    date,
    AVG(temperature_2m) as avg_temp,
    MAX(temperature_2m) as max_temp,
    MIN(temperature_2m) as min_temp,
    SUM(precipitation) as total_rain,
    AVG(wind_speed_10m) as avg_wind
FROM weather.forecast_and_current_weather.silver_hourly_forecast_weather
GROUP BY country, city, date
ORDER BY country, city, date;
```

### Detección de Cambios (SCD Type 2)

```sql
-- Identificar cuando cambió un pronóstico
SELECT 
    latitude,
    longitude,
    timestamp,
    temperature_2m,
    query_timestamp,
    __START_AT,
    __END_AT,
    DATEDIFF(minute, __START_AT, __END_AT) as minutes_valid
FROM weather.forecast_and_current_weather.bronze_hourly_forecast_weather_scd_type2
WHERE __END_AT IS NOT NULL
  AND temperature_2m IS NOT NULL
ORDER BY latitude, longitude, timestamp, __START_AT DESC;
```

## 🎯 Casos de Uso

1. **Dashboards en Tiempo Real** - Monitoreo de clima actual
2. **Análisis Predictivo** - Planificación basada en pronósticos
3. **Alertas Meteorológicas** - Detección de condiciones severas
4. **Auditoría de Pronósticos** - Comparar predicciones vs realidad
5. **Análisis de Cambios** - Cómo evolucionan los pronósticos

## 📝 Notas Técnicas

### SCD Type 2 vs Streaming Tables Regulares

**SCD Type 2 (`bronze_hourly_forecast_weather_scd_type2`):**
* ✅ Mantiene historial completo de cambios
* ✅ Permite auditar evolución de pronósticos
* ✅ Columnas `__START_AT` y `__END_AT`
* ⚠️ Mayor uso de almacenamiento

**Streaming Tables (`silver_hourly_forecast_weather`):**
* ✅ Siempre muestra estado más reciente
* ✅ Menor footprint de almacenamiento
* ❌ No rastrea cambios históricos

### Liquid Clustering

El dataset SCD Type 2 usa liquid clustering en `[latitude, longitude]`:
* ✅ Optimiza filtros por ubicación
* ✅ Mejora performance de joins
* ✅ Auto-optimización sin Z-ORDER manual

### Materialized Views

Las vistas materializadas en Silver y Gold:
* ✅ Refresh incremental automático (en serverless)
* ✅ Optimizadas para queries analíticos
* ✅ Reducen latencia de consulta

## 🔗 Referencias

* [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
* [Databricks Lakeflow Pipelines](https://docs.databricks.com/workflows/delta-live-tables/index.html)
* [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
* [Change Data Capture](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

---

**Autor:** Jose Quesada  
**Email:** jaquesada92@outlook.com  
**Last Updated:** Junio 19, 2026  
**Pipeline ID:** `22324c07-f417-4e26-b944-994a7f2e9d77`
