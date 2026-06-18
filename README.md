# Taller UTP Databricks - Pipeline de Mediciones del Clima

Este proyecto implementa pipelines de datos usando arquitectura Medallion (Bronze → Silver) para procesar mediciones meteorológicas en formato JSON, incluyendo datos en tiempo real, históricos y pronósticos.

## 📋 Descripción

El proyecto procesa tres tipos de datos del clima:

### 🌤️ Datos en Tiempo Real (Current Weather)
- Mediciones actuales del clima
- Actualización continua en tiempo real
- Incluye: temperatura, humedad, precipitación, viento, presión, etc.

### 📚 Datos Históricos (Historical Weather)
- Registros históricos de mediciones pasadas
- Para análisis de tendencias y patrones
- Datos agregados por períodos de tiempo

### 🔮 Pronósticos (Forecast Weather)
- Predicciones meteorológicas futuras
- Pronósticos a corto, mediano y largo plazo
- Incluye probabilidades y rangos de confianza

**Datos procesados incluyen:**
- Coordenadas geográficas (latitud, longitud, elevación)
- Temperatura y humedad relativa
- Precipitación y lluvia
- Velocidad y dirección del viento
- Código de clima y estado día/noche
- Timestamps de las mediciones

## 🏗️ Arquitectura

```
CATÁLOGO: weather
│
├── SCHEMA: current_weather (Tiempo Real)
│   ├── data/mediciones_del_clima/current/*.json
│   │   ↓
│   ├── [Bronze Layer] bronze_current_weather
│   │   ↓
│   └── [Silver Layer] silver_current_weather
│
├── SCHEMA: historical_weather (Histórico)
│   ├── data/mediciones_del_clima/historical/*.json
│   │   ↓
│   ├── [Bronze Layer] bronze_historical_weather
│   │   ↓
│   └── [Silver Layer] silver_historical_weather
│
└── SCHEMA: forecast_weather (Pronóstico)
    ├── data/mediciones_del_clima/forecast/*.json
    │   ↓
    ├── [Bronze Layer] bronze_forecast_weather
    │   ↓
    └── [Silver Layer] silver_forecast_weather
```

## 📂 Estructura del Proyecto

```
taller-utp-databricks/
├── README.md                              # Este archivo
├── setup.sql                              # Script de configuración inicial
├── data/
│   └── mediciones_del_clima/
│       ├── current/                       # Archivos JSON - Tiempo real
│       ├── historical/                    # Archivos JSON - Histórico
│       └── forecast/                      # Archivos JSON - Pronósticos
├── transformations/
│   ├── current_weather.py                 # Pipeline de datos actuales
│   ├── historical_weather.py              # Pipeline de datos históricos
│   └── forecast_weather.py                # Pipeline de pronósticos
├── script/                                # Scripts auxiliares
├── utils.py                               # Utilidades
├── open_meteo_weather_ingestion.py        # Script de ingesta
└── generate_ingestion_files               # Notebook de generación
```

## ⚙️ Configuración Paso a Paso

### 🚀 Opción Rápida: Usar el Script de Setup

**La forma más fácil de configurar el proyecto es usando el script `setup.sql`:**

1. Abre un **nuevo notebook SQL** en Databricks
2. Copia todo el contenido del archivo `setup.sql`
3. Ejecuta todas las celdas en orden
4. Continúa con el **Paso 3** para crear los pipelines

El script `setup.sql` es idempotente (puedes ejecutarlo múltiples veces sin problemas) y automáticamente:
- ✅ Crea el catálogo `weather`
- ✅ Crea los 3 schemas: `current_weather`, `historical_weather`, `forecast_weather`
- ✅ Verifica la configuración
- ✅ Muestra información útil sobre la estructura creada

---

### Configuración Manual (Alternativa)

Si prefieres crear la infraestructura manualmente, sigue estos pasos:

### Paso 1: Crear el Catálogo Unity Catalog

1. Abre el **Data Explorer** en Databricks o ejecuta en un notebook:

```sql
-- Crear el catálogo
CREATE CATALOG IF NOT EXISTS weather
COMMENT 'Catálogo para datos meteorológicos del taller UTP';

-- Verificar que se creó
SHOW CATALOGS LIKE 'weather';
```

### Paso 2: Crear los Schemas

```sql
-- Usar el catálogo
USE CATALOG weather;

-- Schema 1: Datos en tiempo real
CREATE SCHEMA IF NOT EXISTS current_weather
COMMENT 'Schema para mediciones actuales del clima en tiempo real';

-- Schema 2: Datos históricos
CREATE SCHEMA IF NOT EXISTS historical_weather
COMMENT 'Schema para datos históricos del clima';

-- Schema 3: Pronósticos
CREATE SCHEMA IF NOT EXISTS forecast_weather
COMMENT 'Schema para pronósticos del clima';

-- Verificar que se crearon
SHOW SCHEMAS IN weather;
```

### Paso 3: Crear los Pipelines

Debes crear **3 pipelines separados**, uno para cada schema:

#### Pipeline 1: Current Weather (Tiempo Real)

1. Ve a **Workflows** → **Lakeflow Pipelines** → **Create Pipeline**
2. Configura:
   - **Pipeline name**: `weather-current-pipeline`
   - **Product edition**: `Advanced` (o `Core`)
   - **Source code**: `/Workspace/Users/<tu-usuario>/taller-utp-databricks/transformations`
   - **Target catalog**: `weather`
   - **Target schema**: `current_weather`
3. En **Configuration**, agrega:
   ```
   username: <tu-email-databricks>
   ```

#### Pipeline 2: Historical Weather (Histórico)

1. Crea un nuevo pipeline
2. Configura:
   - **Pipeline name**: `weather-historical-pipeline`
   - **Product edition**: `Advanced` (o `Core`)
   - **Source code**: `/Workspace/Users/<tu-usuario>/taller-utp-databricks/transformations`
   - **Target catalog**: `weather`
   - **Target schema**: `historical_weather`
3. En **Configuration**, agrega:
   ```
   username: <tu-email-databricks>
   ```

#### Pipeline 3: Forecast Weather (Pronóstico)

1. Crea un nuevo pipeline
2. Configura:
   - **Pipeline name**: `weather-forecast-pipeline`
   - **Product edition**: `Advanced` (o `Core`)
   - **Source code**: `/Workspace/Users/<tu-usuario>/taller-utp-databricks/transformations`
   - **Target catalog**: `weather`
   - **Target schema**: `forecast_weather`
3. En **Configuration**, agrega:
   ```
   username: <tu-email-databricks>
   ```

### Paso 4: Verificar las Tablas Creadas

Ejecuta estas consultas en un notebook para verificar:

```sql
-- Ver todas las tablas creadas en cada schema
SHOW TABLES IN weather.current_weather;
SHOW TABLES IN weather.historical_weather;
SHOW TABLES IN weather.forecast_weather;

-- Verificar datos en las tablas de tiempo real
SELECT 
  date,
  time,
  temperature_2m,
  precipitation,
  wind_speed_10m
FROM weather.current_weather.silver_current_weather
ORDER BY date DESC, time DESC
LIMIT 10;

-- Verificar datos históricos
SELECT 
  date,
  temperature_2m,
  precipitation
FROM weather.historical_weather.silver_historical_weather
ORDER BY date DESC
LIMIT 10;

-- Verificar pronósticos
SELECT 
  date,
  time,
  temperature_2m,
  precipitation
FROM weather.forecast_weather.silver_forecast_weather
ORDER BY date ASC, time ASC
LIMIT 10;
```

## 🔍 Descripción de los Schemas y Tablas

### Schema: current_weather

**Propósito**: Mediciones meteorológicas en tiempo real

#### bronze_current_weather
- **Tipo**: Streaming Table
- **Fuente**: Auto Loader (cloudFiles) sobre archivos JSON
- **Schema Evolution**: Habilitado
- **Datos**: Crudos sin transformación

#### silver_current_weather
- **Tipo**: Streaming Table
- **Transformaciones**:
  - Conversión de timestamp Unix a columnas `date` y `time`
  - Limpieza y validación de datos
  - Organización de columnas por categoría

### Schema: historical_weather

**Propósito**: Datos históricos para análisis de tendencias

#### bronze_historical_weather
- **Tipo**: Streaming Table
- **Fuente**: Auto Loader sobre archivos JSON históricos
- **Datos**: Registros pasados sin procesar

#### silver_historical_weather
- **Tipo**: Streaming Table
- **Transformaciones**:
  - Agregaciones temporales
  - Cálculo de estadísticas (promedios, máximos, mínimos)
  - Formato optimizado para consultas analíticas

### Schema: forecast_weather

**Propósito**: Pronósticos meteorológicos futuros

#### bronze_forecast_weather
- **Tipo**: Streaming Table
- **Fuente**: Auto Loader sobre archivos JSON de pronósticos
- **Datos**: Predicciones crudas del clima

#### silver_forecast_weather
- **Tipo**: Streaming Table
- **Transformaciones**:
  - Separación de fecha y hora
  - Organización de predicciones por horizonte temporal
  - Cálculo de intervalos de confianza

## 🚨 Troubleshooting

### Error: "Catalog 'weather' does not exist"

**Solución**: Ejecuta el script `setup.sql` o crea el catálogo manualmente:

```sql
CREATE CATALOG IF NOT EXISTS weather;
```

### Error: "Schema 'current_weather' / 'historical_weather' / 'forecast_weather' does not exist"

**Solución**: Ejecuta el script `setup.sql` completo que crea los 3 schemas, o créalos manualmente:

```sql
USE CATALOG weather;
CREATE SCHEMA IF NOT EXISTS current_weather;
CREATE SCHEMA IF NOT EXISTS historical_weather;
CREATE SCHEMA IF NOT EXISTS forecast_weather;
```

### Error: "Pipeline configuration parameter 'username' not found"

**Solución**: 
1. Ve a Pipeline Settings → Configuration
2. Agrega el parámetro `username` con tu correo de Databricks

### Error: "No files found in path"

**Solución**: Verifica que los archivos JSON existen en las carpetas correspondientes:
```
/Workspace/Users/<tu-usuario>/taller-utp-databricks/data/mediciones_del_clima/current/
/Workspace/Users/<tu-usuario>/taller-utp-databricks/data/mediciones_del_clima/historical/
/Workspace/Users/<tu-usuario>/taller-utp-databricks/data/mediciones_del_clima/forecast/
```

### Los pipelines se interfieren entre sí

**Solución**: Asegúrate de que cada pipeline apunta al schema correcto:
- Pipeline current → schema `current_weather`
- Pipeline historical → schema `historical_weather`
- Pipeline forecast → schema `forecast_weather`

## 📊 Consultas de Ejemplo

### Comparar temperatura actual vs pronóstico

```sql
SELECT 
  c.date,
  c.time,
  c.temperature_2m as temp_actual,
  f.temperature_2m as temp_pronosticada,
  ROUND(ABS(c.temperature_2m - f.temperature_2m), 2) as diferencia
FROM weather.current_weather.silver_current_weather c
INNER JOIN weather.forecast_weather.silver_forecast_weather f
  ON c.date = f.date 
  AND c.time = f.time
  AND c.latitude = f.latitude
  AND c.longitude = f.longitude
ORDER BY c.date DESC, c.time DESC
LIMIT 20;
```

### Análisis de tendencias históricas

```sql
SELECT 
  date,
  ROUND(AVG(temperature_2m), 2) as temp_promedio,
  ROUND(MIN(temperature_2m), 2) as temp_minima,
  ROUND(MAX(temperature_2m), 2) as temp_maxima,
  ROUND(AVG(precipitation), 2) as precipitacion_promedio
FROM weather.historical_weather.silver_historical_weather
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

### Precisión de pronósticos

```sql
WITH pronosticos_vs_real AS (
  SELECT 
    h.date,
    h.temperature_2m as temp_real,
    f.temperature_2m as temp_pronosticada,
    ABS(h.temperature_2m - f.temperature_2m) as error_absoluto
  FROM weather.historical_weather.silver_historical_weather h
  INNER JOIN weather.forecast_weather.silver_forecast_weather f
    ON h.date = f.date
    AND h.latitude = f.latitude
    AND h.longitude = f.longitude
)
SELECT 
  COUNT(*) as num_pronosticos,
  ROUND(AVG(error_absoluto), 2) as error_promedio,
  ROUND(MAX(error_absoluto), 2) as error_maximo,
  ROUND(STDDEV(error_absoluto), 2) as desviacion_estandar
FROM pronosticos_vs_real;
```

### Alertas de condiciones extremas (tiempo real)

```sql
SELECT 
  date,
  time,
  latitude,
  longitude,
  temperature_2m,
  wind_speed_10m,
  precipitation,
  CASE 
    WHEN temperature_2m > 35 THEN '🔥 Calor Extremo'
    WHEN temperature_2m < 0 THEN '❄️ Congelamiento'
    WHEN wind_speed_10m > 60 THEN '💨 Vientos Fuertes'
    WHEN precipitation > 50 THEN '🌧️ Lluvia Intensa'
    ELSE '✅ Normal'
  END as alerta
FROM weather.current_weather.silver_current_weather
WHERE temperature_2m > 35 
   OR temperature_2m < 0 
   OR wind_speed_10m > 60 
   OR precipitation > 50
ORDER BY date DESC, time DESC;
```

## 🔄 Mantenimiento

### Actualizar pipelines después de cambios

1. Edita los archivos en `transformations/`:
   - `current_weather.py`
   - `historical_weather.py`
   - `forecast_weather.py`
2. Ve al pipeline correspondiente en la UI
3. Haz clic en **Start** para aplicar cambios

### Agregar nuevos datos

Coloca archivos JSON en las carpetas correspondientes:
- Tiempo real: `data/mediciones_del_clima/current/`
- Histórico: `data/mediciones_del_clima/historical/`
- Pronóstico: `data/mediciones_del_clima/forecast/`

El Auto Loader detectará automáticamente los nuevos archivos.

### Refresh completo de todas las tablas

```sql
-- Current Weather
REFRESH TABLE weather.current_weather.bronze_current_weather;
REFRESH TABLE weather.current_weather.silver_current_weather;

-- Historical Weather
REFRESH TABLE weather.historical_weather.bronze_historical_weather;
REFRESH TABLE weather.historical_weather.silver_historical_weather;

-- Forecast Weather
REFRESH TABLE weather.forecast_weather.bronze_forecast_weather;
REFRESH TABLE weather.forecast_weather.silver_forecast_weather;
```

## 📝 Notas Importantes

1. **Unity Catalog**: Este proyecto usa Unity Catalog. Asegúrate de tener permisos para crear catálogos y schemas.

2. **Múltiples Schemas**: El proyecto usa 3 schemas independientes para separar datos en tiempo real, históricos y pronósticos.

3. **Auto Loader**: Todas las tablas Bronze usan Auto Loader para detección automática de nuevos archivos.

4. **Streaming Tables**: Todas las tablas son streaming tables para procesamiento incremental y eficiente.

5. **Pipelines Independientes**: Cada schema debe tener su propio pipeline configurado correctamente.

6. **Script de Setup**: El archivo `setup.sql` crea toda la infraestructura de Unity Catalog necesaria.

## 🤝 Soporte

Para problemas o preguntas sobre este proyecto:
1. Revisa la sección de Troubleshooting
2. Verifica los logs de cada pipeline en la UI de Databricks
3. Consulta la documentación oficial de Databricks Lakeflow Pipelines

## 📚 Referencias

- [Databricks Lakeflow Pipelines Documentation](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

**Última actualización**: 2026-06-15
**Versión del Pipeline**: 1.0
**Schemas**: current_weather, historical_weather, forecast_weather
**Autor**: Taller UTP Databricks
