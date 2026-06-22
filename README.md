# Open-Meteo Databricks Pipeline

Pipeline de datos meteorológicos en tiempo real usando Databricks Lakeflow Spark Declarative Pipelines (SDP) con arquitectura Medallion para procesar datos de pronósticos del clima desde la API Open-Meteo.

## 📋 Descripción General

Este proyecto implementa un pipeline ETL completo que:
* Ingiere datos de pronósticos meteorológicos desde la API Open-Meteo
* Procesa datos usando arquitectura Medallion (Bronze → Silver → Gold)
* Implementa Change Data Capture (CDC) con SCD Type 2 para rastrear cambios históricos
* Proporciona datos limpios y enriquecidos para análisis y visualización
* **Incluye dashboard de análisis de precisión de pronósticos (Real vs Forecast)**

## 🏗️ Arquitectura de Datos y Lineage

### Pipeline Unificado de Pronóstico y Clima Actual

```
📁 Archivos JSON (/data/forecast/hourly/*.json y /data/forecast/current/*.json)
        ↓ [Auto Loader - cloudFiles]
        
🟤 BRONZE LAYER
├─ raw_hourly_forecast_weather (Streaming Table)
│  └─ Ingesta cruda de pronósticos horarios con Auto Loader
│  └─ Schema evolution habilitado
│  └─ 13,824 registros
│  
       ↓ [Auto CDC Flow]
       
├─ bronze_hourly_forecast_weather_scd_type2 (Streaming Table - SCD Type 2)
│  └─ Rastreo de cambios históricos en pronósticos
│  └─ Keys: [latitude, longitude, timestamp]
│  └─ Sequence by: query_timestamp
│  └─ Liquid Clustering: [latitude, longitude]
│  └─ Columnas rastreadas: weather_code, precipitation, rain, temperatures, wind_direction
│
├─ bronze_current_weather (Streaming Table)
│  └─ Ingesta cruda de clima actual con Auto Loader
│  
       ↓ [Transformaciones + Enriquecimiento]
       
⚪ SILVER LAYER
├─ silver_hourly_forecast_weather (Materialized View)
│  └─ Pronósticos transformados y enriquecidos
│  └─ Timestamps parseados (date, hour)
│  └─ Enriquecido con weather_codes
│  └─ Descripciones de dirección de viento
│  └─ 10,824 registros
│
├─ silver_current_weather (Streaming Table)
│  └─ Clima actual transformado y parseado
│  └─ 3,621 registros
│  
       ↓ [Union + Filtros temporales + Join con locations]
       
🟡 GOLD LAYER
└─ gold_current_and_forecast_weather (Materialized View)
   └─ Vista unificada de clima actual y pronósticos
   └─ Join con tabla de ubicaciones
   └─ Información geográfica enriquecida (país, provincia, ciudad)
   └─ Filtros temporales aplicados
   └─ 493 registros
```

### Tablas de Referencia

```
📋 REFERENCE TABLES
├─ locations (25 ubicaciones)
│  └─ Información geográfica (país, provincia, ciudad, coordenadas)
│  └─ Usado en joins de capa Gold
│
└─ weather_codes
   └─ Diccionario de códigos de clima
   └─ Categorías e intensidades
   └─ Broadcast join en Silver layer
```

## 📊 Datasets y Schemas

### Catálogo: `weather`
#### Schema: `open_meteo`

| Dataset | Tipo | Layer | Filas | Descripción |
|---------|------|-------|-------|-------------|
| `raw_hourly_forecast_weather` | Streaming Table | Bronze | 13,824 | Ingesta cruda de pronósticos horarios |
| `bronze_hourly_forecast_weather_scd_type2` | Streaming Table (SCD2) | Bronze | - | Histórico de cambios en pronósticos |
| `bronze_current_weather` | Streaming Table | Bronze | - | Ingesta cruda de clima actual |
| `silver_hourly_forecast_weather` | Materialized View | Silver | 10,824 | Pronósticos transformados y enriquecidos |
| `silver_current_weather` | Streaming Table | Silver | 3,621 | Clima actual transformado |
| `gold_current_and_forecast_weather` | Materialized View | Gold | 493 | Vista unificada de clima actual y pronósticos |
| `locations` | Table | Reference | 25 | Catálogo de ubicaciones en Panamá |
| `weather_codes` | Table | Reference | - | Códigos de clima WMO |

### Columnas Clave

**Coordenadas y Ubicación:**
* `latitude`, `longitude`, `elevation`
* `country`, `province`, `city`, `place_name`
* `timezone`, `timezone_abbreviation`

**Temporal:**
* `timestamp` - Timestamp completo del pronóstico/medición
* `date` - Fecha (DATE)
* `hour` - Hora (HH:mm:ss)
* `query_timestamp` - Momento de la consulta API
* `__START_AT`, `__END_AT` - Validez temporal (solo SCD Type 2)

**Variables Meteorológicas:**
* `temperature_2m`, `temperature_80m`, `temperature_120m`, `temperature_180m` - Temperaturas en °C
* `precipitation`, `rain`, `showers` - Precipitación en mm
* `precipitation_probability` - Probabilidad de precipitación (%)
* `relative_humidity_2m` - Humedad relativa (%)
* `pressure_msl` - Presión al nivel del mar (hPa)
* `weather_code` - Código WMO del clima
* `weather_category`, `weather_intensity` - Categorización del clima
* `wind_speed_10m`, `wind_speed_80m`, `wind_speed_120m`, `wind_speed_180m` **(en nudos)**
* `wind_direction_10m`, `wind_direction_80m`, `wind_direction_120m`, `wind_direction_180m` (grados)
* `description_wind_direction_*` - Descripciones textuales (N, NE, E, SE, S, SW, W, NW)
* `wind_gusts_10m` **(en nudos)**

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
* **Lakeview Dashboards** - Visualización y análisis interactivo

### Procesamiento
* **PySpark** - Transformaciones distribuidas
* **Streaming Tables** - Procesamiento continuo
* **Materialized Views** - Vistas optimizadas con refresh incremental
* **Metric Views** - Agregaciones y métricas para dashboards

### API y Datos
* **Open-Meteo API** - Proveedor de datos meteorológicos
* **requests-cache** - Cache de respuestas API (1 hora)
* **retry-requests** - Reintentos automáticos con backoff exponencial

## ⚙️ Configuración del Pipeline

### Pipeline: `dlt_forecast_hourly_weather`

**Settings:**
* **Pipeline Type:** Workspace
* **Target:** `weather.open_meteo`
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

## 📊 Dashboard de Análisis: Forecast vs Real

### Descripción

Dashboard interactivo que compara predicciones meteorológicas con observaciones reales para evaluar la precisión de los pronósticos.

### 🎯 Métricas de Precisión

* **Error Promedio Precipitación**: MAE en milímetros (0.31 mm)
* **Error Promedio Viento**: MAE en nudos
* **RMSE Precipitación**: Root Mean Square Error para precipitación
* **RMSE Viento**: Root Mean Square Error para viento
* **Total de Comparaciones**: Número de pares forecast-real analizados

### 🎛️ Filtros Interactivos

* **Provincia**: Filtro multi-select por provincia
* **Ciudad**: Filtro multi-select por ciudad
* **Spot Name**: Filtro multi-select por ubicación específica
* **Horizonte de Pronóstico**: 
  * "Más de 4 horas" - Pronósticos de largo plazo
  * "4 horas o menos" - Pronósticos de corto plazo

### 📈 Visualizaciones

1. **Scatter Plot - Precipitación Real vs Forecast**
   * Eje X: Precipitación pronosticada (mm)
   * Eje Y: Precipitación real (mm)
   * Diagonal perfecta = predicción exacta

2. **Scatter Plot - Viento Real vs Forecast**
   * Eje X: Velocidad de viento pronosticada (nudos)
   * Eje Y: Velocidad de viento real (nudos)

3. **Gráfico de Error por Horizonte**
   * Muestra cómo varía el error según las horas de anticipación del pronóstico
   * Tendencia esperada: mayor error a mayor distancia temporal

4. **Tabla Detallada de Comparaciones**
   * Timestamp del pronóstico
   * Horas de anticipación (hours_ahead)
   * Ubicación (lat/long)
   * Valores predichos vs reales

### 📐 Metodología

**Comparación de Forecasts vs Observaciones Reales:**

```sql
-- Forecasts: Predicciones hechas ANTES del evento
WHERE query_timestamp < timestamp

-- Observaciones Reales: Mediciones más recientes para el mismo período
WHERE timestamp <= query_timestamp
```

**Métricas Calculadas:**
* **MAE** (Mean Absolute Error): `AVG(ABS(forecast - actual))`
* **RMSE** (Root Mean Square Error): `SQRT(AVG((forecast - actual)²))`
* **Error por Horizonte**: Precisión según anticipación del pronóstico

**Dataset Source:**
* `silver_hourly_forecast_weather` - Fuente de datos con histórico de predicciones y observaciones
* Join con `locations` para información geográfica

## 🚀 Uso

### 1. Configuración Inicial

```sql
-- Crear catálogo si no existe
CREATE CATALOG IF NOT EXISTS weather;

-- Crear schema
CREATE SCHEMA IF NOT EXISTS weather.open_meteo;

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
    place_name,
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
FROM weather.open_meteo.gold_current_and_forecast_weather
WHERE timestamp >= CURRENT_TIMESTAMP()
ORDER BY city, place_name, timestamp;
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
    AVG(wind_speed_10m) as avg_wind,
    AVG(precipitation_probability) as avg_precip_prob
FROM weather.open_meteo.silver_hourly_forecast_weather
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
    precipitation,
    query_timestamp,
    __START_AT,
    __END_AT,
    DATEDIFF(minute, __START_AT, __END_AT) as minutes_valid
FROM weather.open_meteo.bronze_hourly_forecast_weather_scd_type2
WHERE __END_AT IS NOT NULL
  AND temperature_2m IS NOT NULL
ORDER BY latitude, longitude, timestamp, __START_AT DESC;

-- Analizar cuántas veces cambió un pronóstico
SELECT 
    latitude,
    longitude,
    timestamp,
    COUNT(*) as num_revisions,
    MIN(__START_AT) as first_forecast,
    MAX(__END_AT) as last_revision
FROM weather.open_meteo.bronze_hourly_forecast_weather_scd_type2
GROUP BY latitude, longitude, timestamp
HAVING COUNT(*) > 1
ORDER BY num_revisions DESC;
```

### Análisis de Precisión de Pronósticos

```sql
-- Precisión por provincia y horizonte temporal
SELECT 
    province,
    forecast_horizon_category,
    COUNT(*) as total_forecasts,
    ROUND(AVG(ABS(forecast_precipitation - actual_precipitation)), 2) as mae_precipitation_mm,
    ROUND(AVG(ABS(forecast_wind_speed - actual_wind_speed)), 2) as mae_wind_nudos,
    ROUND(SQRT(AVG(POWER(forecast_precipitation - actual_precipitation, 2))), 2) as rmse_precipitation,
    ROUND(SQRT(AVG(POWER(forecast_wind_speed - actual_wind_speed, 2))), 2) as rmse_wind
FROM (
    SELECT 
        l.province,
        CASE 
            WHEN DATEDIFF(HOUR, f.query_timestamp, f.timestamp) > 4 
            THEN 'Más de 4 horas' 
            ELSE '4 horas o menos' 
        END as forecast_horizon_category,
        f.precipitation AS forecast_precipitation,
        a.precipitation AS actual_precipitation,
        f.wind_speed_10m AS forecast_wind_speed,
        a.wind_speed_10m AS actual_wind_speed
    FROM weather.open_meteo.silver_hourly_forecast_weather f
    LEFT JOIN weather.open_meteo.locations l
        ON f.latitude = l.latitude AND f.longitude = l.longitude
    INNER JOIN (
        SELECT 
            latitude, longitude, timestamp, precipitation, wind_speed_10m,
            ROW_NUMBER() OVER (PARTITION BY latitude, longitude, timestamp ORDER BY query_timestamp DESC) AS rn
        FROM weather.open_meteo.silver_hourly_forecast_weather
        WHERE timestamp <= query_timestamp
    ) a
        ON f.latitude = a.latitude
        AND f.longitude = a.longitude
        AND f.timestamp = a.timestamp
        AND a.rn = 1
    WHERE f.query_timestamp < f.timestamp
        AND f.timestamp < CURRENT_TIMESTAMP()
)
GROUP BY province, forecast_horizon_category
ORDER BY province, forecast_horizon_category;
```

### Análisis de Clima por Provincia

```sql
-- Condiciones actuales y pronóstico inmediato por provincia
SELECT 
    province,
    COUNT(DISTINCT place_name) as num_locations,
    AVG(temperature_2m) as avg_temp,
    MAX(temperature_2m) as max_temp,
    MIN(temperature_2m) as min_temp,
    SUM(precipitation) as total_precipitation,
    AVG(wind_speed_10m) as avg_wind_speed,
    MAX(wind_gusts_10m) as max_wind_gusts,
    AVG(relative_humidity_2m) as avg_humidity
FROM weather.open_meteo.gold_current_and_forecast_weather
WHERE timestamp BETWEEN CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP() + INTERVAL 6 HOURS
GROUP BY province
ORDER BY province;
```

## 🎯 Casos de Uso

1. **Dashboards en Tiempo Real** - Monitoreo de clima actual y pronósticos
2. **Análisis Predictivo** - Planificación basada en pronósticos con probabilidades
3. **Alertas Meteorológicas** - Detección de condiciones severas (vientos fuertes, lluvias intensas)
4. **Auditoría de Pronósticos** - Comparar predicciones vs realidad (Dashboard Forecast vs Real)
5. **Análisis de Cambios** - Cómo evolucionan los pronósticos usando SCD Type 2
6. **Evaluación de Modelos** - Métricas de precisión por horizonte temporal y ubicación
7. **Planificación Agrícola** - Predicción de precipitaciones para riego
8. **Turismo y Eventos** - Pronósticos para planificación de actividades al aire libre

## 📝 Notas Técnicas

### SCD Type 2 vs Streaming Tables Regulares

**SCD Type 2 (`bronze_hourly_forecast_weather_scd_type2`):**
* ✅ Mantiene historial completo de cambios
* ✅ Permite auditar evolución de pronósticos
* ✅ Columnas `__START_AT` y `__END_AT`
* ✅ **Habilitador del análisis Forecast vs Real**
* ✅ Permite analizar cuántas veces se revisó un pronóstico
* ⚠️ Mayor uso de almacenamiento

**Streaming Tables (`silver_hourly_forecast_weather`):**
* ✅ Siempre muestra estado más reciente
* ✅ Menor footprint de almacenamiento
* ❌ No rastrea cambios históricos

### Liquid Clustering

El dataset SCD Type 2 usa liquid clustering en `[latitude, longitude]`:
* ✅ Optimiza filtros por ubicación
* ✅ Mejora performance de joins con tabla `locations`
* ✅ Auto-optimización sin Z-ORDER manual
* ✅ Beneficia queries del dashboard de análisis

### Materialized Views

Las vistas materializadas en Silver y Gold:
* ✅ Refresh incremental automático (en serverless)
* ✅ Optimizadas para queries analíticos
* ✅ Reducen latencia de consulta
* ✅ `gold_current_and_forecast_weather` unifica clima actual y pronósticos

### Unidades de Medida

* **Temperatura**: Celsius (°C)
* **Precipitación y Lluvia**: Milímetros (mm)
* **Viento**: Nudos (knots)
* **Presión**: hectoPascales (hPa)
* **Humedad**: Porcentaje (%)
* **Probabilidad de Precipitación**: Porcentaje (%)

## 🔗 Referencias

* [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
* [Databricks Lakeflow Pipelines](https://docs.databricks.com/workflows/delta-live-tables/index.html)
* [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
* [Change Data Capture](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Lakeview Dashboards](https://docs.databricks.com/dashboards/index.html)
* [Liquid Clustering](https://docs.databricks.com/delta/clustering.html)

---

**Autor:** Jose Quesada  
**Email:** jaquesada92@outlook.com  
**Last Updated:** Junio 19, 2026  
**Pipeline ID:** `22324c07-f417-4e26-b944-994a7f2e9d77`  
**Catálogo:** `weather`  
**Schema:** `open_meteo`
