# 03 - Arquitectura e Integración con Servicios Azure

## 📝 Índice
- [Rol de Databricks en la Arquitectura](#rol-de-databricks-en-la-arquitectura)
- [Patrones de Arquitectura Comunes](#patrones-de-arquitectura-comunes)
- [Integración con Azure Cosmos DB](#integración-con-azure-cosmos-db)
- [Integración con Azure Storage](#integración-con-azure-storage)
- [Integración con Azure AI Search](#integración-con-azure-ai-search)
- [Integración con Otros Servicios Azure](#integración-con-otros-servicios-azure)
- [Arquitectura de Referencia Completa](#arquitectura-de-referencia-completa)

---

## Rol de Databricks en la Arquitectura

### Posicionamiento en la Arquitectura de Datos Moderna

```
┌─────────────────────────────────────────────────────────────────┐
│                   ARQUITECTURA DE DATOS                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Fuentes de Datos]                                             │
│       │                                                         │
│       ├── Bases de datos transaccionales                        │
│       ├── APIs y servicios externos                             │
│       ├── IoT devices y sensores                                │
│       ├── Archivos (CSV, JSON, Parquet)                         │
│       └── Streaming (Event Hubs, Kafka)                         │
│       │                                                         │
│       ▼                                                         │
│  ┌──────────────────────────────────────┐                      │
│  │    CAPA DE INGESTA                   │                      │
│  │  - Azure Data Factory                │                      │
│  │  - Event Hubs                        │                      │
│  │  - IoT Hub                           │                      │
│  └──────────────┬───────────────────────┘                      │
│                 │                                               │
│                 ▼                                               │
│  ┌──────────────────────────────────────┐                      │
│  │    ALMACENAMIENTO RAW                │                      │
│  │  - Azure Data Lake Storage Gen2      │                      │
│  │  - Blob Storage                      │                      │
│  └──────────────┬───────────────────────┘                      │
│                 │                                               │
│                 ▼                                               │
│  ╔═════════════════════════════════════╗                       │
│  ║   ⚡ AZURE DATABRICKS               ║  ← CAPA CENTRAL       │
│  ║                                     ║                       │
│  ║  • Limpieza y transformación        ║                       │
│  ║  • Enriquecimiento de datos         ║                       │
│  ║  • Agregaciones y métricas          ║                       │
│  ║  • Machine Learning                 ║                       │
│  ║  • Validación de calidad            ║                       │
│  ║                                     ║                       │
│  ║  Bronze → Silver → Gold             ║                       │
│  ╚═════════════┬═══════════════════════╝                       │
│                │                                               │
│                ▼                                               │
│  ┌──────────────────────────────────────┐                      │
│  │    CAPA DE SERVICIO                  │                      │
│  │  - Cosmos DB (transaccional)         │                      │
│  │  - Azure SQL (relacional)            │                      │
│  │  - AI Search (búsqueda)              │                      │
│  │  - Synapse (warehouse)               │                      │
│  └──────────────┬───────────────────────┘                      │
│                 │                                               │
│                 ▼                                               │
│  [Consumidores]                                                 │
│       │                                                         │
│       ├── Power BI (dashboards)                                 │
│       ├── Aplicaciones web/móvil                                │
│       ├── APIs RESTful                                          │
│       └── Sistemas downstream                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Databricks como "Motor de Transformación"

Databricks actúa como el **cerebro analítico** que:

1. **Recibe** datos de múltiples fuentes
2. **Procesa** y transforma a escala masiva
3. **Enriquece** con lógica de negocio y ML
4. **Distribuye** a sistemas de consumo especializados

---

## Patrones de Arquitectura Comunes

### Patrón 1: Lambda Architecture (Batch + Stream)

```
┌──────────────────────────────────────────────────────┐
│            LAMBDA ARCHITECTURE                       │
├──────────────────────────────────────────────────────┤
│                                                      │
│  [Fuentes de Datos]                                  │
│         │                                            │
│         ├─────────────┬─────────────┐                │
│         │             │             │                │
│         ▼             ▼             │                │
│   ┌─────────┐   ┌──────────┐       │                │
│   │ Batch   │   │ Stream   │       │                │
│   │ (ADLS)  │   │(EventHub)│       │                │
│   └────┬────┘   └─────┬────┘       │                │
│        │              │             │                │
│        ▼              ▼             │                │
│   ┌─────────┐   ┌──────────┐       │                │
│   │ Batch   │   │ Stream   │       │                │
│   │ Layer   │   │ Layer    │       │                │
│   │ (Jobs)  │   │(Str.Jobs)│       │                │
│   └────┬────┘   └─────┬────┘       │                │
│        │              │             │                │
│        └──────┬───────┘             │                │
│               ▼                     │                │
│        ┌────────────┐               │                │
│        │  Serving   │               │                │
│        │   Layer    │               │                │
│        │(Cosmos DB) │               │                │
│        └────────────┘               │                │
│                                     │                │
└─────────────────────────────────────┘                │
```

**Ejemplo de implementación**:

```python
# BATCH LAYER - Job programado cada hora
# Notebook: /Production/batch_processing.py

from pyspark.sql.functions import col, window, sum, avg

# Leer datos históricos
df_batch = spark.read \
    .format("delta") \
    .load("/mnt/bronze/transactions/")

# Agregaciones complejas
daily_aggregates = df_batch \
    .groupBy("date", "category", "region") \
    .agg(
        sum("amount").alias("total_sales"),
        avg("amount").alias("avg_ticket"),
        count("*").alias("num_transactions")
    )

# Escribir a serving layer
daily_aggregates.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"date >= '{current_date}'") \
    .save("/mnt/gold/daily_metrics")

# COMMAND ----------

# STREAM LAYER - Procesamiento continuo
# Notebook: /Production/stream_processing.py

# Leer stream de Event Hub
df_stream = spark.readStream \
    .format("eventhubs") \
    .options(**eventhub_config) \
    .load()

# Procesar en ventanas de 5 minutos
realtime_metrics = df_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("category")
    ) \
    .agg(
        sum("amount").alias("realtime_sales"),
        count("*").alias("realtime_count")
    )

# Escribir a Delta para consultas en tiempo real
query = realtime_metrics.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/realtime") \
    .start("/mnt/gold/realtime_metrics")
```

### Patrón 2: Medallion Architecture (Bronze-Silver-Gold)

```
┌────────────────────────────────────────────────────┐
│         MEDALLION ARCHITECTURE                     │
├────────────────────────────────────────────────────┤
│                                                    │
│  ┌──────────────────────────────────────────┐     │
│  │  BRONZE LAYER (Raw Data)                 │     │
│  │  • Copia exacta de fuente               │     │
│  │  • Sin transformaciones                  │     │
│  │  • Histórico completo                    │     │
│  │  • Formato: JSON, CSV, Parquet           │     │
│  └──────────────┬───────────────────────────┘     │
│                 │                                  │
│                 ▼                                  │
│  ┌──────────────────────────────────────────┐     │
│  │  SILVER LAYER (Cleaned & Conformed)      │     │
│  │  • Datos limpios y validados             │     │
│  │  • Esquema estandarizado                 │     │
│  │  • Deduplicación                         │     │
│  │  • Formato: Delta Lake                   │     │
│  └──────────────┬───────────────────────────┘     │
│                 │                                  │
│                 ▼                                  │
│  ┌──────────────────────────────────────────┐     │
│  │  GOLD LAYER (Business Aggregates)        │     │
│  │  • Métricas de negocio                   │     │
│  │  • Agregaciones                          │     │
│  │  • Listos para consumo                   │     │
│  │  • Formato: Delta Lake                   │     │
│  └──────────────────────────────────────────┘     │
│                                                    │
└────────────────────────────────────────────────────┘
```

**Implementación completa**:

```python
# ============================================
# BRONZE LAYER - Ingesta sin transformación
# ============================================

# Leer datos raw de Azure Storage
df_bronze = spark.read \
    .format("json") \
    .option("multiLine", "true") \
    .load("abfss://raw@storage.dfs.core.windows.net/sales/2026/05/*/")

# Agregar metadatos de ingesta
from pyspark.sql.functions import current_timestamp, input_file_name

df_bronze_enriched = df_bronze \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# Escribir a Bronze (sin cambios en los datos)
df_bronze_enriched.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .save("/mnt/bronze/sales")

# ============================================
# SILVER LAYER - Limpieza y conformado
# ============================================

from pyspark.sql.functions import col, trim, upper, to_date, when

# Leer desde Bronze
df_silver = spark.read \
    .format("delta") \
    .load("/mnt/bronze/sales")

# Aplicar transformaciones
df_silver_clean = df_silver \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("amount") > 0) \
    .withColumn("customer_name", trim(upper(col("customer_name")))) \
    .withColumn("transaction_date", to_date(col("transaction_timestamp"))) \
    .withColumn("amount", col("amount").cast("decimal(18,2)")) \
    .withColumn("status", 
        when(col("status").isNull(), "PENDING")
        .otherwise(col("status"))
    )

# Escribir a Silver
df_silver_clean.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("transaction_date") \
    .option("mergeSchema", "true") \
    .save("/mnt/silver/sales")

# ============================================
# GOLD LAYER - Agregaciones de negocio
# ============================================

# Leer desde Silver
df_gold = spark.read \
    .format("delta") \
    .load("/mnt/silver/sales")

# Crear métricas agregadas
sales_summary = df_gold \
    .groupBy("transaction_date", "category", "region") \
    .agg(
        sum("amount").alias("total_sales"),
        count("transaction_id").alias("num_transactions"),
        avg("amount").alias("avg_transaction"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Escribir a Gold
sales_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .save("/mnt/gold/sales_summary")

# Crear tabla para SQL queries
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.sales_summary
    USING DELTA
    LOCATION '/mnt/gold/sales_summary'
""")
```

---

## Integración con Azure Cosmos DB

### Lectura desde Cosmos DB

```python
# Configuración de conexión a Cosmos DB
cosmos_config = {
    "spark.cosmos.accountEndpoint": dbutils.secrets.get("keyvault", "cosmos-endpoint"),
    "spark.cosmos.accountKey": dbutils.secrets.get("keyvault", "cosmos-key"),
    "spark.cosmos.database": "sales_database",
    "spark.cosmos.container": "transactions"
}

# Lectura completa (batch)
df_cosmos = spark.read \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .load()

display(df_cosmos.limit(10))

# Lectura con filtro (pushdown al servidor)
df_filtered = spark.read \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.read.customQuery", 
            "SELECT * FROM c WHERE c.category = 'Electronics' AND c.amount > 1000") \
    .load()

# Lectura incremental (solo cambios)
df_incremental = spark.read \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.changeFeed.startFrom", "Beginning") \
    .option("spark.cosmos.changeFeed.mode", "Incremental") \
    .load()
```

### Escritura a Cosmos DB

```python
# Preparar datos para escribir
df_to_cosmos = df_processed.select(
    col("id").cast("string"),
    col("customer_id"),
    col("product_name"),
    col("amount"),
    col("transaction_date"),
    col("status")
)

# Escritura batch
df_to_cosmos.write \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .mode("append") \
    .save()

# Escritura con upsert (actualizar si existe, insertar si no)
df_to_cosmos.write \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.write.strategy", "ItemOverwriteIfNotModified") \
    .option("spark.cosmos.write.bulk.enabled", "true") \
    .mode("append") \
    .save()

print("✅ Datos escritos a Cosmos DB exitosamente")
```

### Patrón: Cosmos DB como Serving Layer

```python
# Pipeline completo: ADLS → Databricks → Cosmos DB

# 1. Leer datos del Data Lake
df_raw = spark.read.parquet("/mnt/bronze/events/")

# 2. Transformar y agregar
df_aggregated = df_raw \
    .groupBy("customer_id", "date") \
    .agg(
        sum("amount").alias("daily_total"),
        count("*").alias("num_transactions"),
        collect_list("product_id").alias("products_purchased")
    )

# 3. Preparar para Cosmos (agregar partition key)
df_for_cosmos = df_aggregated \
    .withColumn("id", concat(col("customer_id"), lit("_"), col("date"))) \
    .withColumn("partitionKey", col("customer_id"))

# 4. Escribir a Cosmos DB para servir a aplicaciones
df_for_cosmos.write \
    .format("cosmos.oltp") \
    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .option("spark.cosmos.database", "analytics") \
    .option("spark.cosmos.container", "customer_daily_summary") \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .mode("append") \
    .save()
```

---

## Integración con Azure Storage

### Conexión a Azure Data Lake Storage Gen2

```python
# Método 1: Service Principal (Recomendado para producción)
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get("keyvault", "sp-client-id")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get("keyvault", "sp-client-secret")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# Método 2: Access Key (Más simple, menos seguro)
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get("keyvault", "storage-key")
)

# Método 3: SAS Token
spark.conf.set(
    f"fs.azure.sas.{container}.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get("keyvault", "sas-token")
)
```

### Operaciones con ADLS Gen2

```python
# Leer diferentes formatos
# CSV
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/*.csv")

# JSON
df_json = spark.read \
    .option("multiLine", "true") \
    .json(f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/*.json")

# Parquet
df_parquet = spark.read \
    .parquet(f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/*.parquet")

# Delta Lake
df_delta = spark.read \
    .format("delta") \
    .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/delta/table")

# Escribir datos
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"abfss://{container}@{storage_account}.dfs.core.windows.net/output/")

# Optimizaciones para escritura
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .option("optimizeWrite", "true") \
    .option("autoCompact", "true") \
    .save(f"abfss://{container}@{storage_account}.dfs.core.windows.net/delta/optimized")
```

---

## Integración con Azure AI Search

### Indexar Datos en AI Search

```python
# Preparar datos para Azure AI Search
from pyspark.sql.functions import concat_ws, col

df_for_search = df_processed.select(
    col("id").cast("string").alias("key"),
    col("title"),
    col("description"),
    col("category"),
    concat_ws(" ", col("title"), col("description")).alias("searchable_text"),
    col("tags"),
    col("last_updated").cast("string")
)

# Convertir a formato JSON
df_json = df_for_search.toJSON().collect()

# Enviar a Azure AI Search via API
import requests
import json

search_endpoint = dbutils.secrets.get("keyvault", "search-endpoint")
search_key = dbutils.secrets.get("keyvault", "search-admin-key")
index_name = "products"

headers = {
    "Content-Type": "application/json",
    "api-key": search_key
}

# Batch upload
batch_size = 1000
documents = [json.loads(doc) for doc in df_json]

for i in range(0, len(documents), batch_size):
    batch = documents[i:i + batch_size]
    payload = {
        "value": [{"@search.action": "upload", **doc} for doc in batch]
    }
    
    response = requests.post(
        f"{search_endpoint}/indexes/{index_name}/docs/index?api-version=2023-11-01",
        headers=headers,
        json=payload
    )
    
    if response.status_code == 200:
        print(f"✅ Batch {i//batch_size + 1} indexed successfully")
    else:
        print(f"❌ Error: {response.text}")

print(f"✅ Total de {len(documents)} documentos indexados")
```

### Pipeline Completo: ADLS → Databricks → AI Search

```python
# 1. Leer documentos desde Data Lake
df_documents = spark.read \
    .format("delta") \
    .load("/mnt/silver/documents")

# 2. Enriquecer con metadatos y procesamiento
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import ArrayType, StringType

# UDF para extraer keywords
@udf(returnType=ArrayType(StringType()))
def extract_keywords(text):
    # Lógica simple de ejemplo
    stopwords = {'el', 'la', 'de', 'y', 'en', 'a'}
    words = text.lower().split()
    return [w for w in words if w not in stopwords and len(w) > 3]

df_enriched = df_documents \
    .withColumn("keywords", extract_keywords(col("content"))) \
    .withColumn("content_length", length(col("content"))) \
    .withColumn("searchable_content", 
        concat_ws(" ", col("title"), col("description"), col("content"))
    )

# 3. Preparar para indexación
df_search_ready = df_enriched.select(
    col("document_id").alias("id"),
    col("title"),
    col("description"),
    col("searchable_content"),
    col("category"),
    col("keywords"),
    col("author"),
    col("created_date"),
    col("content_length")
)

# 4. Exportar y cargar a Azure AI Search
# (código de indexación anterior)
```

---

## Integración con Otros Servicios Azure

### Azure Synapse Analytics

```python
# Escribir a Synapse (dedicated SQL pool)
df_result.write \
    .format("sqldw") \
    .option("url", f"jdbc:sqlserver://{synapse_server}.sql.azuresynapse.net:1433") \
    .option("tempDir", f"abfss://temp@{storage}.dfs.core.windows.net/synapse") \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("dbTable", "dbo.sales_summary") \
    .option("user", dbutils.secrets.get("keyvault", "synapse-user")) \
    .option("password", dbutils.secrets.get("keyvault", "synapse-password")) \
    .mode("overwrite") \
    .save()
```

### Azure SQL Database

```python
# Leer desde Azure SQL
jdbc_url = f"jdbc:sqlserver://{sql_server}.database.windows.net:1433;database={database}"
connection_properties = {
    "user": dbutils.secrets.get("keyvault", "sql-user"),
    "password": dbutils.secrets.get("keyvault", "sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df_sql = spark.read \
    .jdbc(url=jdbc_url, table="dbo.customers", properties=connection_properties)

# Escribir a Azure SQL
df_to_sql.write \
    .jdbc(url=jdbc_url, table="dbo.analytics_results", 
          mode="append", properties=connection_properties)
```

### Event Hubs (Streaming)

```python
# Configuración de Event Hub
eventhub_config = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        dbutils.secrets.get("keyvault", "eventhub-connection-string")
    ),
    "eventhubs.consumerGroup": "$Default"
}

# Leer stream
df_stream = spark.readStream \
    .format("eventhubs") \
    .options(**eventhub_config) \
    .load()

# Procesar stream
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", StringType())
])

df_parsed = df_stream \
    .select(from_json(col("body").cast("string"), schema).alias("data")) \
    .select("data.*")

# Escribir resultados
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/iot") \
    .start("/mnt/gold/iot_data")
```

---

## Arquitectura de Referencia Completa

### Ejemplo: Sistema de Análisis de Ventas

```
┌──────────────────────────────────────────────────────────────────────┐
│                     ARQUITECTURA COMPLETA                            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   E-commerce │  │  Mobile App  │  │  POS System  │              │
│  │   Website    │  │              │  │              │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                  │                      │
│         └─────────────────┴──────────────────┘                      │
│                           │                                         │
│                           ▼                                         │
│                  ┌─────────────────┐                                │
│                  │  Event Hubs     │  (Real-time)                   │
│                  └────────┬────────┘                                │
│                           │                                         │
│         ┌─────────────────┴─────────────────┐                      │
│         │                                   │                      │
│         ▼                                   ▼                      │
│  ┌──────────────┐                  ┌──────────────────┐            │
│  │ Azure Data   │  (Batch)         │  Streaming       │            │
│  │ Factory      │───────┐          │  Ingestion       │            │
│  └──────────────┘       │          └────────┬─────────┘            │
│                         │                   │                      │
│                         ▼                   │                      │
│                  ┌──────────────────────────▼──────┐               │
│                  │   Azure Data Lake Gen2          │               │
│                  │                                 │               │
│                  │  /bronze/  (Raw Data)           │               │
│                  └────────┬────────────────────────┘               │
│                           │                                        │
│                           ▼                                        │
│           ╔═══════════════════════════════════════════╗            │
│           ║      ⚡ AZURE DATABRICKS                  ║            │
│           ║                                           ║            │
│           ║  ┌─────────────────────────────────────┐ ║            │
│           ║  │  Job 1: Bronze → Silver             │ ║            │
│           ║  │  - Limpieza                         │ ║            │
│           ║  │  - Validación                       │ ║            │
│           ║  │  - Deduplicación                    │ ║            │
│           ║  └────────────┬────────────────────────┘ ║            │
│           ║               │                          ║            │
│           ║  ┌────────────▼────────────────────────┐ ║            │
│           ║  │  Job 2: Silver → Gold               │ ║            │
│           ║  │  - Agregaciones                     │ ║            │
│           ║  │  - Métricas de negocio              │ ║            │
│           ║  │  - ML predictions                   │ ║            │
│           ║  └────────────┬────────────────────────┘ ║            │
│           ║               │                          ║            │
│           ║  ┌────────────▼────────────────────────┐ ║            │
│           ║  │  Job 3: Export                      │ ║            │
│           ║  │  - Cosmos DB (apps)                 │ ║            │
│           ║  │  - AI Search (búsqueda)             │ ║            │
│           ║  │  - Synapse (warehouse)              │ ║            │
│           ║  └─────────────────────────────────────┘ ║            │
│           ╚═══════════════════════════════════════════╝            │
│                           │                                        │
│         ┌─────────────────┼─────────────────┐                     │
│         │                 │                 │                     │
│         ▼                 ▼                 ▼                     │
│  ┌──────────┐    ┌───────────────┐  ┌──────────────┐            │
│  │ Cosmos DB│    │ Azure AI      │  │   Synapse    │            │
│  │          │    │ Search        │  │   Analytics  │            │
│  │(Real-time│    │(Full-text     │  │  (Warehouse) │            │
│  │  serving)│    │ search)       │  │              │            │
│  └────┬─────┘    └───────┬───────┘  └──────┬───────┘            │
│       │                  │                  │                    │
│       └──────────────────┴──────────────────┘                    │
│                          │                                       │
│                          ▼                                       │
│              ┌───────────────────────┐                           │
│              │    CONSUMIDORES       │                           │
│              │                       │                           │
│              │  - Power BI           │                           │
│              │  - Web Apps           │                           │
│              │  - Mobile Apps        │                           │
│              │  - APIs               │                           │
│              └───────────────────────┘                           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Checklist de Integración

Al diseñar una solución con Databricks, verificar:

- [ ] **Fuentes de datos** identificadas y accesibles
- [ ] **Autenticación** configurada (Service Principal, Managed Identity)
- [ ] **Almacenamiento** montado o accesible via ABFS/WASB
- [ ] **Medallion architecture** definida (Bronze, Silver, Gold)
- [ ] **Particionamiento** estratégico para optimizar consultas
- [ ] **Formatos** apropiados (Delta Lake para transaccional)
- [ ] **Destinos** de datos configurados (Cosmos, SQL, Search)
- [ ] **Monitoring** y logging habilitados
- [ ] **Secrets** gestionados via Key Vault
- [ ] **Networking** configurado (Private Endpoints si aplica)

---

**Anterior**: [02 - Componentes Clave](./02-componentes-clave.md)  
**Siguiente**: [04 - Clusters y Gestión de Costos](./04-clusters-costos.md)
