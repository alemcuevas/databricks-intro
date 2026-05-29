# Demo 01 - Recorrido por un Notebook de Transformación

## 🎯 Objetivo de la Demo

Mostrar un notebook de transformación completo que:
- Lee datos desde múltiples fuentes (Cosmos DB y Azure Storage)
- Aplica transformaciones y lógica de negocio
- Implementa el patrón Medallion (Bronze → Silver → Gold)
- Escribe resultados a destinos finales

## ⏱️ Duración

30 minutos

---

## 📋 Contexto del Escenario

**Caso de Uso**: Sistema de análisis de ventas en tiempo real

**Flujo de Datos**:
```
[E-commerce Site] → [Cosmos DB] → [Databricks] → [Delta Lake] → [Power BI]
[POS Systems]     → [ADLS Gen2] → [Databricks] → [Cosmos DB]   → [Web App]
```

**Requisitos de Negocio**:
1. Consolidar datos de ventas de múltiples canales
2. Calcular métricas agregadas diarias
3. Identificar productos con bajo inventario
4. Detectar anomalías en ventas
5. Preparar datos para visualización en Power BI

---

## 📓 Notebook Completo: ETL de Ventas

### Celda 1: Metadatos y Documentación

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Pipeline: Ventas Consolidadas
# MAGIC 
# MAGIC ## 📋 Información del Notebook
# MAGIC 
# MAGIC | Campo | Valor |
# MAGIC |-------|-------|
# MAGIC | **Propósito** | Consolidar y transformar datos de ventas de múltiples fuentes |
# MAGIC | **Frecuencia** | Diario (3:00 AM) |
# MAGIC | **Fuentes** | Cosmos DB (online), ADLS Gen2 (POS) |
# MAGIC | **Destinos** | Delta Lake (Gold), Cosmos DB (serving) |
# MAGIC | **Duración Típica** | 15-20 minutos |
# MAGIC | **Última Actualización** | 2026-05-22 |
# MAGIC | **Mantenedor** | Data Engineering Team |
# MAGIC 
# MAGIC ## 🔄 Cambios Recientes
# MAGIC 
# MAGIC - **2026-05-20**: Agregado cálculo de anomalías en ventas
# MAGIC - **2026-05-15**: Optimizado join entre online y POS
# MAGIC - **2026-05-10**: Agregado soporte para múltiples regiones
```

---

### Celda 2: Configuración e Imports

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Configuración Inicial

# COMMAND ----------

# Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# Configuración de logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fecha de procesamiento (puede venir como parámetro)
try:
    processing_date = dbutils.widgets.get("fecha_proceso")
except:
    # Widget no existe, usar fecha actual
    dbutils.widgets.text("fecha_proceso", "2026-05-22", "Fecha de Proceso")
    processing_date = dbutils.widgets.get("fecha_proceso")

logger.info(f"🚀 Iniciando procesamiento para fecha: {processing_date}")

# Variables de configuración
storage_account = "mystorageaccount"
container_bronze = "bronze"
container_silver = "silver"
container_gold = "gold"

# Secrets desde Key Vault
cosmos_endpoint = dbutils.secrets.get("keyvault", "cosmos-endpoint")
cosmos_key = dbutils.secrets.get("keyvault", "cosmos-key")
storage_key = dbutils.secrets.get("keyvault", "storage-key")

print(f"""
✅ Configuración cargada:
   - Fecha de proceso: {processing_date}
   - Storage account: {storage_account}
   - Cosmos endpoint: {cosmos_endpoint[:30]}...
""")
```

---

### Celda 3: Bronze Layer - Ingesta de Datos

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ BRONZE LAYER: Ingesta de Datos Sin Transformación

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Lectura desde Cosmos DB (Ventas Online)

# COMMAND ----------

# Configuración de Cosmos DB
cosmos_config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "sales",
    "spark.cosmos.container": "online_transactions"
}

# Leer datos del día especificado
df_online_bronze = spark.read \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.read.customQuery",
            f"SELECT * FROM c WHERE c.sale_date = '{processing_date}'") \
    .load()

# Agregar metadatos de ingesta
df_online_bronze = df_online_bronze \
    .withColumn("source_system", lit("online")) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("processing_date", lit(processing_date))

count_online = df_online_bronze.count()
logger.info(f"📊 Registros de ventas online: {count_online:,}")

# Guardar en Bronze
path_bronze_online = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/sales/online/{processing_date}/"

df_online_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(path_bronze_online)

print(f"✅ Datos online guardados en Bronze: {path_bronze_online}")

# Vista previa
display(df_online_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Lectura desde ADLS Gen2 (Ventas POS)

# COMMAND ----------

# Configurar acceso a Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Leer archivos CSV de POS del día
path_pos_raw = f"abfss://raw@{storage_account}.dfs.core.windows.net/pos/{processing_date}/*.csv"

df_pos_bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(path_pos_raw)

# Agregar metadatos
df_pos_bronze = df_pos_bronze \
    .withColumn("source_system", lit("pos")) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("processing_date", lit(processing_date))

count_pos = df_pos_bronze.count()
logger.info(f"📊 Registros de ventas POS: {count_pos:,}")

# Guardar en Bronze
path_bronze_pos = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/sales/pos/{processing_date}/"

df_pos_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(path_bronze_pos)

print(f"✅ Datos POS guardados en Bronze: {path_bronze_pos}")

# Vista previa
display(df_pos_bronze.limit(5))

# COMMAND ----------

# Resumen de Bronze Layer
print(f"""
📦 BRONZE LAYER - Resumen de Ingesta
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fecha de proceso: {processing_date}
Ventas Online:    {count_online:,} registros
Ventas POS:       {count_pos:,} registros
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL:            {count_online + count_pos:,} registros
""")
```

---

### Celda 4: Silver Layer - Limpieza y Conformado

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ SILVER LAYER: Limpieza, Validación y Conformado

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Leer desde Bronze

# COMMAND ----------

# Leer ambas fuentes desde Bronze
df_online_raw = spark.read.format("delta").load(path_bronze_online)
df_pos_raw = spark.read.format("delta").load(path_bronze_pos)

print(f"📖 Leyendo desde Bronze:")
print(f"   Online: {df_online_raw.count():,} registros")
print(f"   POS: {df_pos_raw.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Estandarizar Esquemas

# COMMAND ----------

# Estandarizar esquema de ventas online
df_online_standard = df_online_raw.select(
    col("transaction_id").cast("string").alias("transaction_id"),
    col("customer_id").cast("string").alias("customer_id"),
    col("product_id").cast("string").alias("product_id"),
    col("quantity").cast("integer").alias("quantity"),
    col("unit_price").cast("decimal(18,2)").alias("unit_price"),
    (col("quantity") * col("unit_price")).alias("total_amount"),
    col("sale_date").cast("date").alias("sale_date"),
    col("sale_timestamp").cast("timestamp").alias("sale_timestamp"),
    col("region").alias("region"),
    col("source_system"),
    col("ingestion_timestamp")
)

# Estandarizar esquema de ventas POS (nombres diferentes)
df_pos_standard = df_pos_raw.select(
    col("txn_id").cast("string").alias("transaction_id"),
    col("cust_id").cast("string").alias("customer_id"),
    col("prod_id").cast("string").alias("product_id"),
    col("qty").cast("integer").alias("quantity"),
    col("price").cast("decimal(18,2)").alias("unit_price"),
    (col("qty") * col("price")).alias("total_amount"),
    to_date(col("sale_dt"), "yyyy-MM-dd").alias("sale_date"),
    col("sale_ts").cast("timestamp").alias("sale_timestamp"),
    col("store_region").alias("region"),
    col("source_system"),
    col("ingestion_timestamp")
)

print("✅ Esquemas estandarizados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Limpieza y Validación

# COMMAND ----------

def clean_and_validate(df, source_name):
    """Aplicar reglas de limpieza y validación"""
    
    logger.info(f"🧹 Limpiando datos de {source_name}...")
    
    # Registros iniciales
    count_initial = df.count()
    
    # 1. Eliminar duplicados
    df_clean = df.dropDuplicates(["transaction_id"])
    count_after_dedup = df_clean.count()
    removed_dups = count_initial - count_after_dedup
    
    # 2. Filtrar registros inválidos
    df_clean = df_clean.filter(
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        (col("total_amount") > 0) &
        (col("customer_id").isNotNull()) &
        (col("product_id").isNotNull())
    )
    count_after_validation = df_clean.count()
    removed_invalid = count_after_dedup - count_after_validation
    
    # 3. Normalizar texto
    df_clean = df_clean \
        .withColumn("region", trim(upper(col("region")))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("product_id", trim(col("product_id")))
    
    # 4. Agregar flag de calidad
    df_clean = df_clean.withColumn("quality_flag", lit("PASSED"))
    
    # Log de resultados
    logger.info(f"""
    {source_name} - Resultados de limpieza:
       Iniciales:          {count_initial:,}
       Duplicados:         {removed_dups:,}
       Inválidos:          {removed_invalid:,}
       Finales (limpios):  {count_after_validation:,}
    """)
    
    return df_clean

# Aplicar limpieza a ambos datasets
df_online_clean = clean_and_validate(df_online_standard, "Online")
df_pos_clean = clean_and_validate(df_pos_standard, "POS")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Unión de Fuentes

# COMMAND ----------

# Combinar ambas fuentes
df_silver = df_online_clean.unionByName(df_pos_clean)

count_silver = df_silver.count()
logger.info(f"📊 Total registros en Silver: {count_silver:,}")

# Vista previa
display(df_silver.orderBy(col("sale_timestamp").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Enriquecimiento con Datos Maestros

# COMMAND ----------

# Leer catálogo de productos (ejemplo)
df_products = spark.read \
    .format("delta") \
    .load(f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/master/products/")

# Enriquecer con información de producto
df_silver_enriched = df_silver.join(
    df_products.select("product_id", "product_name", "category", "brand"),
    on="product_id",
    how="left"
)

# Calcular métricas adicionales
df_silver_enriched = df_silver_enriched \
    .withColumn("sale_hour", hour(col("sale_timestamp"))) \
    .withColumn("sale_day_of_week", dayofweek(col("sale_timestamp"))) \
    .withColumn("is_weekend", 
        when(col("sale_day_of_week").isin([1, 7]), True).otherwise(False)
    )

print("✅ Datos enriquecidos con catálogo de productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Guardar Silver Layer

# COMMAND ----------

path_silver = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/sales/consolidated/"

# Escribir particionado por fecha y región para optimizar queries
df_silver_enriched.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("sale_date", "region") \
    .option("mergeSchema", "true") \
    .option("optimizeWrite", "true") \
    .save(path_silver)

logger.info(f"✅ Silver Layer guardado: {path_silver}")

# Crear tabla para SQL queries
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.sales_consolidated
    USING DELTA
    LOCATION '{path_silver}'
""")

print(f"""
🥈 SILVER LAYER - Resumen
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Registros procesados: {count_silver:,}
Particiones:          fecha + región
Formato:              Delta Lake
Tabla SQL:            silver.sales_consolidated
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
```

---

### Celda 5: Gold Layer - Agregaciones de Negocio

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ GOLD LAYER: Métricas y Agregaciones de Negocio

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Métricas Diarias por Región y Categoría

# COMMAND ----------

# Leer desde Silver
df_silver_full = spark.read \
    .format("delta") \
    .load(path_silver) \
    .filter(col("sale_date") == processing_date)

# Agregaciones diarias
df_daily_metrics = df_silver_full \
    .groupBy("sale_date", "region", "category") \
    .agg(
        sum("total_amount").alias("total_sales"),
        count("transaction_id").alias("num_transactions"),
        avg("total_amount").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_units_sold"),
        min("sale_timestamp").alias("first_sale"),
        max("sale_timestamp").alias("last_sale")
    ) \
    .withColumn("processing_timestamp", current_timestamp())

display(df_daily_metrics.orderBy(col("total_sales").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Top Productos por Ventas

# COMMAND ----------

df_top_products = df_silver_full \
    .groupBy("sale_date", "product_id", "product_name", "category") \
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("units_sold")
    ) \
    .withColumn("rank",
        row_number().over(
            Window.partitionBy("sale_date", "category")
            .orderBy(col("total_sales").desc())
        )
    ) \
    .filter(col("rank") <= 10)

display(df_top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Detección de Anomalías

# COMMAND ----------

# Calcular estadísticas históricas (últimos 30 días)
historical_start = (datetime.strptime(processing_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")

df_historical = spark.read \
    .format("delta") \
    .load(path_silver) \
    .filter((col("sale_date") >= historical_start) & (col("sale_date") < processing_date))

# Estadísticas por categoría y región
df_historical_stats = df_historical \
    .groupBy("region", "category") \
    .agg(
        avg("total_amount").alias("avg_amount_30d"),
        stddev("total_amount").alias("stddev_amount_30d")
    )

# Identificar transacciones anómalas (> 3 desviaciones estándar)
df_with_anomaly_flag = df_silver_full \
    .join(df_historical_stats, on=["region", "category"], how="left") \
    .withColumn("z_score",
        (col("total_amount") - col("avg_amount_30d")) / col("stddev_amount_30d")
    ) \
    .withColumn("is_anomaly",
        when(abs(col("z_score")) > 3, True).otherwise(False)
    )

# Ver anomalías detectadas
df_anomalies = df_with_anomaly_flag.filter(col("is_anomaly") == True)
count_anomalies = df_anomalies.count()

if count_anomalies > 0:
    logger.warning(f"⚠️ {count_anomalies} transacciones anómalas detectadas")
    display(df_anomalies.select(
        "transaction_id", "product_name", "total_amount", 
        "avg_amount_30d", "z_score"
    ).orderBy(col("z_score").desc()))
else:
    print("✅ No se detectaron anomalías")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Guardar Gold Layer

# COMMAND ----------

# Guardar métricas diarias
path_gold_daily = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net/sales/daily_metrics/"

df_daily_metrics.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("sale_date") \
    .save(path_gold_daily)

# Guardar top productos
path_gold_top_products = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net/sales/top_products/"

df_top_products.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("sale_date") \
    .save(path_gold_top_products)

# Guardar anomalías
if count_anomalies > 0:
    path_gold_anomalies = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net/sales/anomalies/"
    
    df_anomalies.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("sale_date") \
        .save(path_gold_anomalies)

# Crear tablas SQL
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.sales_daily_metrics
    USING DELTA
    LOCATION '{path_gold_daily}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.sales_top_products
    USING DELTA
    LOCATION '{path_gold_top_products}'
""")

logger.info("✅ Gold Layer guardado exitosamente")

print(f"""
🥇 GOLD LAYER - Resumen
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Métricas diarias:     {df_daily_metrics.count():,} registros
Top productos:        {df_top_products.count():,} registros
Anomalías detectadas: {count_anomalies:,} registros
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
```

---

### Celda 6: Export a Sistemas Downstream

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ EXPORT: Escribir a Sistemas de Consumo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Export a Cosmos DB (Serving Layer para Apps)

# COMMAND ----------

# Preparar resumen para Cosmos DB (API transaccional)
df_for_cosmos = df_daily_metrics.select(
    concat(
        col("sale_date"),
        lit("_"),
        col("region"),
        lit("_"),
        col("category")
    ).alias("id"),
    col("sale_date").cast("string").alias("date"),
    col("region").alias("partitionKey"),  # Partition key
    col("category"),
    col("total_sales"),
    col("num_transactions"),
    col("avg_transaction_value"),
    col("unique_customers"),
    col("processing_timestamp").cast("string")
)

# Escribir a Cosmos DB
cosmos_write_config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "analytics",
    "spark.cosmos.container": "daily_sales_summary"
}

df_for_cosmos.write \
    .format("cosmos.oltp") \
    .options(**cosmos_write_config) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .mode("append") \
    .save()

logger.info("✅ Datos exportados a Cosmos DB")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Optimización de Tablas Delta

# COMMAND ----------

# Optimizar tablas para mejor rendimiento en queries
from delta.tables import DeltaTable

# Optimize + Z-Ordering en Silver
DeltaTable.forPath(spark, path_silver) \
    .optimize() \
    .executeZOrderBy("region", "category")

# Optimize en Gold
DeltaTable.forPath(spark, path_gold_daily) \
    .optimize() \
    .executeCompaction()

# Vacuum para limpiar archivos antiguos (retener 7 días)
DeltaTable.forPath(spark, path_silver) \
    .vacuum(168)  # 168 horas = 7 días

logger.info("✅ Tablas Delta optimizadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Resumen Final y Notificación

# COMMAND ----------

# Calcular métricas del pipeline
end_time = datetime.now()
start_time_str = spark.conf.get("spark.databricks.clusterUsageTags.startTime", "Unknown")

# Crear resumen ejecutivo
summary = {
    "fecha_proceso": processing_date,
    "timestamp_fin": end_time.isoformat(),
    "registros_procesados": {
        "bronze_online": count_online,
        "bronze_pos": count_pos,
        "silver": count_silver,
        "gold_metrics": df_daily_metrics.count(),
        "anomalias": count_anomalies
    },
    "total_ventas": float(df_daily_metrics.agg(sum("total_sales")).collect()[0][0]),
    "status": "SUCCESS"
}

# Imprimir resumen
print(f"""
╔════════════════════════════════════════════════════════╗
║        RESUMEN DE EJECUCIÓN - ETL VENTAS              ║
╠════════════════════════════════════════════════════════╣
║                                                       ║
║  📅 Fecha:                    {processing_date}                  ║
║  ⏱️ Hora finalización:         {end_time.strftime("%H:%M:%S")}                    ║
║                                                       ║
║  📊 REGISTROS PROCESADOS:                             ║
║     Bronze (Online):          {count_online:>10,}             ║
║     Bronze (POS):             {count_pos:>10,}             ║
║     Silver (consolidado):     {count_silver:>10,}             ║
║     Gold (métricas):          {df_daily_metrics.count():>10,}             ║
║                                                       ║
║  💰 Ventas Totales:           ${summary['total_ventas']:>12,.2f}        ║
║  ⚠️ Anomalías Detectadas:     {count_anomalies:>10,}             ║
║                                                       ║
║  ✅ STATUS:                    {summary['status']}                       ║
║                                                       ║
╚════════════════════════════════════════════════════════╝
""")

# Retornar resumen en JSON para orquestación
dbutils.notebook.exit(json.dumps(summary))
```

---

## 🎯 Puntos Clave de la Demo

Durante la demostración, enfatiza:

### 1. Organización y Documentación
- ✅ Celdas Markdown para documentación clara
- ✅ Secciones bien definidas
- ✅ Comentarios explicativos en el código

### 2. Manejo de Configuración
- ✅ Uso de Secrets para credenciales
- ✅ Widgets para parametrización
- ✅ Variables de configuración centralizadas

### 3. Patrón Medallion
- ✅ Bronze: datos sin transformar
- ✅ Silver: datos limpios y conformados
- ✅ Gold: métricas de negocio

### 4. Calidad de Datos
- ✅ Validaciones
- ✅ Deduplicación
- ✅ Detección de anomalías
- ✅ Logging de resultados

### 5. Optimizaciones
- ✅ Particionamiento estratégico
- ✅ Delta Lake optimizations
- ✅ Z-ordering para queries
- ✅ Vacuum para limpieza

### 6. Observabilidad
- ✅ Logging detallado
- ✅ Métricas de ejecución
- ✅ Resumen ejecutivo
- ✅ Salida estructurada (JSON)

---

## ❓ Preguntas para Hacer al Partner

Durante la demo, preguntar:

1. **Fuentes de Datos**:
   - ¿De dónde vienen realmente sus datos?
   - ¿Qué tan frecuente llegan?
   - ¿Hay otras fuentes además de Cosmos y Storage?

2. **Transformaciones**:
   - ¿Qué lógica de negocio específica aplican?
   - ¿Hay reglas de validación adicionales?
   - ¿Cómo manejan datos históricos?

3. **Destinos**:
   - ¿A dónde se escriben finalmente los datos?
   - ¿Quiénes son los consumidores?
   - ¿Hay SLAs definidos?

4. **Calidad**:
   - ¿Cómo validan que los datos son correctos?
   - ¿Qué hacen cuando hay errores?
   - ¿Hay alertas configuradas?

5. **Operación**:
   - ¿Quién monitorea estos notebooks?
   - ¿Qué hacer si falla el job?
   - ¿Hay documentación de runbooks?

---

## 📚 Recursos Adicionales

- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)
- [Data Quality Patterns](https://docs.databricks.com/data-governance/unity-catalog/data-quality.html)

---

**Siguiente**: [Demo 02 - Ejecución de Job y Revisión de Resultados](./demo-02-job-ejecucion.md)
