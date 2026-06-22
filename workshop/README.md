# 🎓 Workshop: Fundamentos de PySpark y Databricks Pipelines
## Construcción de un Pipeline ETL en 1 Hora

---

## 📚 Descripción General

Taller práctico de 1 hora donde aprenderás los conceptos fundamentales de PySpark y Databricks construyendo un pipeline ETL real basado en datos meteorológicos.

**Basado en:** Open-Meteo Databricks Pipeline (proyecto de producción real)

---

## ⏱️ Agenda del Taller (60 minutos)

| Módulo | Duración | Tema | Notebook |
|--------|----------|------|----------|
| **1** | 10 min | Unity Catalog y Fundamentos Databricks | `01_unity_catalog_fundamentos.py` |
| **2** | 15 min | PySpark Básico: DataFrames y Transformaciones | `02_pyspark_basico.py` |
| **3** | 15 min | Auto Loader e Ingesta de Datos | `03_auto_loader_ingesta.py` |
| **4** | 15 min | Arquitectura Medallion y SDP Pipelines | `04_medallion_architecture.py` |
| **5** | 5 min | Bonus: Conceptos Avanzados (SCD Type 2) | `05_conceptos_avanzados.py` |

---

## 🎯 Objetivos de Aprendizaje

Al finalizar este taller, serás capaz de:

✅ Comprender la estructura de Unity Catalog (Catalog → Schema → Table)  
✅ Crear y manipular DataFrames con PySpark  
✅ Aplicar transformaciones comunes (select, filter, withColumn, join)  
✅ Usar Auto Loader para ingesta incremental de archivos  
✅ Implementar arquitectura Medallion (Bronze → Silver → Gold)  
✅ Entender los componentes de Lakeflow Spark Declarative Pipelines (SDP)  
✅ Conocer conceptos avanzados como SCD Type 2 y Liquid Clustering  

---

## 📂 Datasets Utilizados

El workshop usa datos del proyecto real **Open-Meteo Pipeline**:

### Dataset Principal
* **Tabla:** `weather.open_meteo.silver_hourly_forecast_weather`
* **Descripción:** Pronósticos meteorológicos horarios transformados
* **Filas:** ~10,000 registros
* **Origen:** API Open-Meteo (25 ubicaciones en Panamá)

### Tablas de Referencia
* `weather.open_meteo.locations` - 25 ubicaciones geográficas
* `weather.open_meteo.weather_codes` - Códigos de clima WMO

### Columnas Clave
* **Ubicación:** `latitude`, `longitude`, `elevation`, `city`, `province`
* **Temporal:** `timestamp`, `date`, `hour`, `query_timestamp`
* **Meteorológicas:** `temperature_2m`, `precipitation`, `wind_speed_10m`, `weather_code`

---

## 🔧 Pre-requisitos

### Conocimientos Previos (Deseables)
* Conceptos básicos de SQL
* Programación Python básica
* Familiaridad con DataFrames (Pandas es útil pero no requerido)

### Requisitos Técnicos
* ✅ Acceso a Databricks Workspace
* ✅ Permisos de lectura en catálogo `weather`
* ✅ Compute/Cluster disponible (Serverless recomendado)

---

## 🚀 Cómo Usar Este Workshop

### Opción 1: Seguir los Notebooks en Orden
Ejecuta cada notebook secuencialmente desde el Módulo 1 hasta el 5.

### Opción 2: Enfoque por Tópico
Si ya tienes experiencia en algunos temas, salta directamente al módulo que te interese:
* ¿Nuevo en Databricks? → Empieza en Módulo 1
* ¿Sabes SQL pero no PySpark? → Módulo 2
* ¿Quieres aprender ingesta? → Módulo 3
* ¿Interesado en arquitectura? → Módulo 4

### Estructura de Cada Notebook
1. **📖 Teoría** - Conceptos clave explicados
2. **💡 Ejemplos Reales** - Código del proyecto en producción
3. **✏️ Ejercicios Prácticos** - Aplica lo aprendido
4. **🔗 Referencias** - Links a documentación oficial

---

## 📊 Arquitectura del Proyecto Base

Este workshop se basa en un pipeline real con arquitectura Medallion:

```
📁 Archivos JSON (data/forecast/hourly/*.json)
        ↓ [Auto Loader]
        
🟤 BRONZE LAYER
└─ raw_hourly_forecast_weather
   └─ Ingesta cruda con schema evolution
   
        ↓ [Transformaciones + Enriquecimiento]
        
⚪ SILVER LAYER
└─ silver_hourly_forecast_weather
   └─ Datos limpios, tipados, enriquecidos
   
        ↓ [Agregaciones + Joins]
        
🟡 GOLD LAYER
└─ gold_current_and_forecast_weather
   └─ Vistas analíticas listas para consumo
```

---

## 🛠️ Tecnologías Cubiertas

* **PySpark** - Procesamiento distribuido de datos
* **Unity Catalog** - Gobierno y organización de datos
* **Auto Loader** - Ingesta incremental con cloudFiles
* **Lakeflow Spark Declarative Pipelines (SDP)** - Framework ETL declarativo
* **Delta Lake** - Formato de tabla con ACID transactions
* **Streaming Tables** - Procesamiento continuo
* **Materialized Views** - Vistas optimizadas
* **Liquid Clustering** - Optimización automática de queries

---

## 📖 Glosario de Términos

| Término | Definición |
|---------|------------|
| **DataFrame** | Estructura de datos distribuida similar a una tabla SQL |
| **Transformation** | Operación que produce un nuevo DataFrame (lazy evaluation) |
| **Action** | Operación que ejecuta transformaciones y retorna resultados |
| **Auto Loader** | Servicio de ingesta incremental de archivos (cloudFiles) |
| **Medallion Architecture** | Patrón Bronze → Silver → Gold para organizar datos |
| **SCD Type 2** | Slowly Changing Dimension - rastrea cambios históricos |
| **Liquid Clustering** | Optimización automática de layout de archivos Delta |

---

## 📚 Recursos Adicionales

### Documentación Oficial
* [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
* [Databricks Documentation](https://docs.databricks.com/)
* [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Lakeflow Pipelines](https://docs.databricks.com/workflows/delta-live-tables/index.html)

### Proyecto Base
* **README Principal:** `/Users/jaquesada92@outlook.com/openmeteo-databricks-pipeline/README.md`
* **Pipeline ID:** `22324c07-f417-4e26-b944-994a7f2e9d77`
* **Código Fuente:** Carpeta `transformations/`

---

## 🎓 Tips para el Instructor

### Ritmo Sugerido
* **Módulos 1-2:** Conceptos fundamentales - asegúrate de que todos entiendan antes de avanzar
* **Módulos 3-4:** Código práctico - deja tiempo para ejecutar células y ver resultados
* **Módulo 5:** Opcional - si el tiempo es ajustado, este módulo puede ser "para llevar"

### Puntos Clave a Enfatizar
1. **Lazy Evaluation** en PySpark - las transformaciones no se ejecutan hasta una acción
2. **Diferencia entre Batch y Streaming** - cuándo usar cada uno
3. **Arquitectura Medallion** - por qué separar Bronze/Silver/Gold
4. **Unity Catalog** - tres niveles: catalog.schema.table

### Troubleshooting Común
* Si un cluster no está disponible → usar Serverless (más rápido)
* Si falta una tabla → verificar permisos en Unity Catalog
* Si un query es lento → agregar `.limit(100)` para demos

---

## ✅ Checklist Pre-Workshop

Antes de iniciar el taller, verifica:

* [ ] Todos tienen acceso al workspace
* [ ] El catálogo `weather` es accesible
* [ ] Hay compute disponible (o Serverless habilitado)
* [ ] Los notebooks se abrieron correctamente
* [ ] Puedes ejecutar: `spark.sql("SELECT * FROM weather.open_meteo.locations LIMIT 5")`

---

## 🤝 Contribuciones

Este workshop es material educativo basado en un proyecto real. Si encuentras errores o tienes sugerencias de mejora, contacta al autor.

---

**Autor:** Jose Quesada  
**Email:** jaquesada92@outlook.com  
**Proyecto Base:** Open-Meteo Databricks Pipeline  
**Última Actualización:** Junio 19, 2026  

---

¡Comencemos! 🚀 Abre el primer notebook: `01_unity_catalog_fundamentos.py`
