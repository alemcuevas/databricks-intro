# 01 - Introducción a Azure Databricks

## 📝 Índice
- [¿Qué es Azure Databricks?](#qué-es-azure-databricks)
- [Historia y Evolución](#historia-y-evolución)
- [Casos de Uso](#casos-de-uso)
- [Ventajas de Azure Databricks](#ventajas-de-azure-databricks)
- [Arquitectura de Alto Nivel](#arquitectura-de-alto-nivel)

---

## ¿Qué es Azure Databricks?

**Azure Databricks** es una plataforma de análisis de datos basada en Apache Spark, optimizada para Microsoft Azure. Es un servicio de análisis unificado que acelera la innovación al unificar los flujos de trabajo de ingeniería de datos, ciencia de datos y análisis empresarial.

### Definición Técnica

Azure Databricks es:
- 🚀 **Plataforma PaaS** (Platform as a Service) totalmente administrada
- ⚡ **Motor de procesamiento distribuido** basado en Apache Spark
- 🔧 **Entorno colaborativo** para científicos de datos, ingenieros y analistas
- 🔄 **Lakehouse unificado** que combina lo mejor de data lakes y data warehouses

### Características Principales

```
┌─────────────────────────────────────────────────────┐
│         AZURE DATABRICKS PLATFORM                   │
├─────────────────────────────────────────────────────┤
│                                                     │
│  📊 Delta Lake    │  🔄 ETL/ELT    │  🤖 ML/AI     │
│  💾 Data Lake     │  📈 Analytics  │  🔍 BI        │
│  ⚡ Streaming     │  🧪 Testing    │  📝 SQL       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## Historia y Evolución

### Línea de Tiempo

| Año | Evento |
|-----|--------|
| **2013** | Creación de Apache Spark en UC Berkeley |
| **2013** | Fundación de Databricks por creadores de Spark |
| **2017** | Lanzamiento de Azure Databricks (partnership Microsoft-Databricks) |
| **2019** | Introducción de Delta Lake |
| **2020** | MLflow integrado nativamente |
| **2022** | Databricks SQL y Unity Catalog |
| **2024** | Mejoras en IA generativa y AutoML |

### Apache Spark: El Motor Subyacente

```python
# Concepto fundamental: Procesamiento distribuido
# Un dataframe de Spark puede procesar terabytes de datos
# distribuidos en cientos de nodos

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IntroduccionDatabricks") \
    .getOrCreate()

# Lectura masiva de datos
df = spark.read.parquet("abfss://container@storage.dfs.core.windows.net/data/")

# Procesamiento distribuido automático
result = df.groupBy("categoria") \
    .agg({"ventas": "sum", "clientes": "count"}) \
    .orderBy("sum(ventas)", ascending=False)
```

---

## Casos de Uso

### 1. 🔄 **ETL/ELT - Ingeniería de Datos**

**Escenario**: Procesamiento de datos transaccionales de múltiples fuentes

```
[Fuentes de Datos]  →  [Databricks]  →  [Data Lake/Warehouse]
     │                      │                    │
  - APIs                 - Limpieza         - Delta Lake
  - Bases de datos       - Transformación   - Cosmos DB
  - Archivos CSV/JSON    - Enriquecimiento  - Synapse
  - Streaming            - Validación       - Power BI
```

**Ejemplo de transformación**:
```python
# ETL: Extraer, Transformar, Cargar
from pyspark.sql.functions import col, when, trim, upper

# Extracción
raw_data = spark.read.json("/mnt/raw/ventas/*.json")

# Transformación
clean_data = raw_data \
    .filter(col("monto") > 0) \
    .withColumn("cliente_nombre", trim(upper(col("cliente")))) \
    .withColumn("categoria", 
        when(col("producto").like("%laptop%"), "Tecnología")
        .when(col("producto").like("%libro%"), "Libros")
        .otherwise("Otros"))

# Carga
clean_data.write \
    .format("delta") \
    .mode("append") \
    .save("/mnt/processed/ventas")
```

### 2. 📊 **Análisis de Datos en Tiempo Real**

**Escenario**: Monitoreo de eventos en streaming (IoT, logs, transacciones)

```python
# Streaming estructurado con Spark
from pyspark.sql.functions import window, avg

# Lectura de stream (Event Hub, Kafka, etc.)
stream_df = spark.readStream \
    .format("eventhubs") \
    .options(**eventhub_config) \
    .load()

# Análisis en ventanas de tiempo
windowed_analysis = stream_df \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperatura").alias("temp_promedio"),
        avg("humedad").alias("humedad_promedio")
    )

# Escritura continua
query = windowed_analysis.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/iot") \
    .start("/mnt/analytics/iot_metrics")
```

### 3. 🤖 **Machine Learning e IA**

**Escenario**: Entrenamiento y despliegue de modelos predictivos

```python
# Pipeline completo de ML con MLflow
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
import mlflow

# Preparación de datos
assembler = VectorAssembler(
    inputCols=["edad", "ingresos", "historial_compras"],
    outputCol="features"
)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

rf = RandomForestClassifier(featuresCol="scaled_features", labelCol="churn")

pipeline = Pipeline(stages=[assembler, scaler, rf])

# Entrenamiento con tracking automático
with mlflow.start_run():
    model = pipeline.fit(training_data)
    
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("num_trees", 100)
    mlflow.spark.log_model(model, "model")
    
    predictions = model.transform(test_data)
    accuracy = evaluator.evaluate(predictions)
    mlflow.log_metric("accuracy", accuracy)
```

### 4. 📈 **Business Intelligence y Reportes**

**Escenario**: Creación de dashboards interactivos y reportes SQL

```sql
-- SQL Analytics en Databricks
-- Análisis de rendimiento de ventas por región

CREATE OR REPLACE VIEW ventas_resumen AS
SELECT 
    region,
    DATE_TRUNC('month', fecha_venta) as mes,
    COUNT(DISTINCT cliente_id) as clientes_unicos,
    SUM(monto_total) as ventas_totales,
    AVG(monto_total) as ticket_promedio,
    SUM(monto_total) - LAG(SUM(monto_total)) OVER (
        PARTITION BY region 
        ORDER BY DATE_TRUNC('month', fecha_venta)
    ) as crecimiento_mes_anterior
FROM ventas_gold
WHERE fecha_venta >= '2026-01-01'
GROUP BY region, DATE_TRUNC('month', fecha_venta)
ORDER BY region, mes;

-- Esta vista se puede consumir directamente en Power BI
```

### 5. 🏗️ **Data Lakehouse**

**Escenario**: Arquitectura moderna de datos unificada

```
┌──────────────────────────────────────────────────┐
│              DATABRICKS LAKEHOUSE                │
├──────────────────────────────────────────────────┤
│                                                  │
│  Bronze Layer (Raw)                              │
│  ├── Datos crudos sin procesar                   │
│  └── Formato original (JSON, CSV, Parquet)       │
│                                                  │
│  Silver Layer (Cleaned & Conformed)              │
│  ├── Datos limpios y validados                   │
│  └── Formato Delta Lake                          │
│                                                  │
│  Gold Layer (Business-Level Aggregates)          │
│  ├── Datos listos para consumo                   │
│  ├── Agregaciones y métricas de negocio          │
│  └── Optimizados para queries analíticos         │
│                                                  │
└──────────────────────────────────────────────────┘
```

---

## Ventajas de Azure Databricks

### 🎯 Integración Nativa con Azure

| Servicio Azure | Integración | Beneficio |
|----------------|-------------|-----------|
| **Azure Data Lake Storage** | Nativa | Almacenamiento escalable y económico |
| **Azure SQL Database** | JDBC/ODBC | Lectura/escritura directa |
| **Cosmos DB** | Conector nativo | NoSQL distribuido globalmente |
| **Azure Synapse** | Delta Lake | Data warehouse unificado |
| **Power BI** | Direct Query | Visualización en tiempo real |
| **Azure DevOps** | CI/CD | Automatización de pipelines |
| **Azure Key Vault** | Secrets | Gestión segura de credenciales |
| **Azure Active Directory** | SCIM | Autenticación y autorización |

### ⚡ Rendimiento y Escalabilidad

```
Escenario Tradicional:
[1 servidor] → 10 horas para procesar 1 TB de datos

Con Databricks:
[100 nodos cluster] → 10 minutos para procesar 1 TB de datos

Escalabilidad:
- Autoscaling: de 2 a 100 nodos automáticamente
- Spot instances: hasta 80% de ahorro en costos
- Delta Cache: 10-100x más rápido en queries repetitivos
```

### 🔒 Seguridad Empresarial

- **Cifrado en reposo y tránsito**: AES-256
- **Network isolation**: VNet injection, Private Link
- **Control de acceso**: Table ACLs, Column-level security
- **Auditoría**: Comprehensive logging
- **Compliance**: GDPR, HIPAA, SOC 2

### 🤝 Colaboración

```
┌─────────────────────────────────────────┐
│      WORKSPACE COLABORATIVO             │
├─────────────────────────────────────────┤
│                                         │
│  👨‍💼 Data Engineers                       │
│  │  └── ETL notebooks, Jobs            │
│  │                                      │
│  👩‍🔬 Data Scientists                      │
│  │  └── ML experiments, Models          │
│  │                                      │
│  👨‍💻 Data Analysts                        │
│  │  └── SQL queries, Dashboards         │
│  │                                      │
│  └──> Compartir: Notebooks, Clusters,  │
│        Resultados, Visualizaciones      │
│                                         │
└─────────────────────────────────────────┘
```

---

## Arquitectura de Alto Nivel

### Control Plane vs Data Plane

```
┌─────────────────────────────────────────────────────────┐
│                  AZURE DATABRICKS                        │
├──────────────────────┬──────────────────────────────────┤
│                      │                                   │
│   CONTROL PLANE      │        DATA PLANE                │
│   (Managed by        │    (Customer's Subscription)      │
│    Databricks)       │                                   │
│                      │                                   │
│  ┌─────────────┐     │    ┌──────────────────────┐      │
│  │ Web UI      │     │    │   VNet (Customer)    │      │
│  │ Job Manager │     │    │                      │      │
│  │ Metadata    │────────→ │  ┌────────────────┐  │      │
│  │ Notebooks   │     │    │  │ Driver Node    │  │      │
│  └─────────────┘     │    │  └────────┬───────┘  │      │
│                      │    │           │          │      │
│                      │    │  ┌────────▼───────┐  │      │
│                      │    │  │ Worker Nodes   │  │      │
│                      │    │  │  (Spark)       │  │      │
│                      │    │  └────────────────┘  │      │
│                      │    └──────────────────────┘      │
│                      │              │                   │
│                      │              ▼                   │
│                      │    ┌──────────────────────┐      │
│                      │    │  Azure Data Lake     │      │
│                      │    │  Cosmos DB           │      │
│                      │    │  Azure SQL           │      │
│                      │    └──────────────────────┘      │
└──────────────────────┴──────────────────────────────────┘
```

### Flujo de Ejecución

1. **Usuario** → Crea/ejecuta notebook en Web UI (Control Plane)
2. **Control Plane** → Instruye al cluster (Data Plane) a ejecutar código
3. **Driver Node** → Coordina la ejecución distribuida
4. **Worker Nodes** → Procesan datos en paralelo
5. **Resultados** → Se almacenan en storage (Delta Lake, etc.)
6. **UI** → Muestra resultados al usuario

---

## 🎯 Resumen de Conceptos Clave

| Concepto | Definición | Importancia |
|----------|------------|-------------|
| **Databricks** | Plataforma unificada de análisis basada en Spark | ⭐⭐⭐⭐⭐ |
| **Apache Spark** | Motor de procesamiento distribuido | ⭐⭐⭐⭐⭐ |
| **Delta Lake** | Capa de almacenamiento con ACID transactions | ⭐⭐⭐⭐⭐ |
| **Workspace** | Entorno colaborativo para equipos | ⭐⭐⭐⭐ |
| **Cluster** | Conjunto de nodos de cómputo | ⭐⭐⭐⭐⭐ |
| **Notebook** | Documento interactivo con código | ⭐⭐⭐⭐ |
| **Job** | Ejecución automatizada de notebooks | ⭐⭐⭐⭐ |
| **Lakehouse** | Arquitectura unificada de datos | ⭐⭐⭐⭐ |

---

## 📚 Recursos Adicionales

- [Documentación oficial de Azure Databricks](https://docs.microsoft.com/azure/databricks/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)

---

**Siguiente**: [02 - Componentes Clave](./02-componentes-clave.md)
