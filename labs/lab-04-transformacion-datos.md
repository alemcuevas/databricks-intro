# Laboratorio 4: Transformación de Datos con Delta Lake

⏱️ **Duración estimada**: 45-50 minutos  
🎯 **Nivel**: Intermedio-Avanzado

## 📋 Objetivos

- Implementar arquitectura Medallion (Bronze → Silver → Gold)
- Dominar operaciones Delta Lake (MERGE, OPTIMIZE, Z-ORDER)
- Aplicar data quality checks
- Usar time travel y versionado
- Optimizar performance con partitioning

---

## Ejercicio 1: Capa Bronze - Ingesta Raw (10 min)

### Paso 1: Crear Datos de Prueba

Notebook: "Lab04_Bronze_Layer"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Bronze: Ingesta Raw

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Simular datos de diferentes fuentes
def generate_sales_data(num_records=1000):
    """Genera datos de ventas con diferentes formatos"""
    dates = [(datetime.now() - timedelta(days=x)) for x in range(30)]
    products = ["LAPTOP", "MOUSE", "KEYBOARD", "MONITOR", "WEBCAM"]
    regions = ["NORTH", "SOUTH", "EAST", "WEST"]
    
    data = []
    for i in range(num_records):
        data.append({
            "transaction_id": f"TXN{i:06d}",
            "date": dates[random.randint(0, len(dates)-1)].strftime("%Y-%m-%d"),
            "product_code": f"P{random.randint(100, 999)}",
            "product_name": random.choice(products),
            "region": random.choice(regions),
            "amount": round(random.uniform(50, 1000), 2),
            "quantity": random.randint(1, 10),
            "customer_id": f"CUST{random.randint(1000, 9999)}",
            "status": random.choice(["completed", "pending", "cancelled"])
        })
    
    return spark.createDataFrame(data)

# COMMAND ----------

# Generar lote 1
df_batch1 = generate_sales_data(500)
print(f"✅ Batch 1: {df_batch1.count()} registros")
display(df_batch1.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardar en Bronze (formato original sin cambios)

# COMMAND ----------

# Agregar metadata de ingesta
df_bronze = df_batch1 \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source", lit("sales_system")) \
    .withColumn("_batch_id", lit("BATCH_001"))

# Definir path Bronze
bronze_path = "/tmp/lab04/bronze/sales"

# Escribir a Delta (modo append para acumular lotes)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .save(bronze_path)

print(f"✅ Guardado en Bronze: {bronze_path}")

# COMMAND ----------

# Verificar tabla Bronze
df_bronze_read = spark.read.format("delta").load(bronze_path)
print(f"📊 Total registros en Bronze: {df_bronze_read.count()}")
display(df_bronze_read.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestar Lote 2 (simulando nueva carga)

# COMMAND ----------

# Generar lote 2 con algunos duplicados intencionales
df_batch2 = generate_sales_data(300)

# Agregar duplicados de batch1 (simular datos repetidos)
duplicates = df_batch1.limit(20).select("transaction_id", "date", "product_code", 
                                         "product_name", "region", "amount", 
                                         "quantity", "customer_id", "status")

df_batch2_with_dups = df_batch2.union(duplicates)

# Agregar metadata
df_bronze_batch2 = df_batch2_with_dups \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source", lit("sales_system")) \
    .withColumn("_batch_id", lit("BATCH_002"))

# Append a Bronze
df_bronze_batch2.write \
    .format("delta") \
    .mode("append") \
    .save(bronze_path)

print(f"✅ Batch 2 agregado a Bronze")

# COMMAND ----------

# Verificar total
df_bronze_all = spark.read.format("delta").load(bronze_path)
print(f"📊 Total registros en Bronze: {df_bronze_all.count()}")

# Verificar duplicados
duplicates_count = df_bronze_all.groupBy("transaction_id").count().filter("count > 1").count()
print(f"⚠️  Transacciones duplicadas: {duplicates_count}")
```

---

## Ejercicio 2: Capa Silver - Limpieza y Deduplicación (15 min)

### Paso 1: Transformaciones Silver

Notebook: "Lab04_Silver_Layer"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Silver: Limpieza y Validación

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Leer Bronze
bronze_path = "/tmp/lab04/bronze/sales"
df_bronze = spark.read.format("delta").load(bronze_path)

print(f"📊 Registros Bronze: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Deduplicación

# COMMAND ----------

# Estrategia: Mantener el registro más reciente por transaction_id
window_spec = Window.partitionBy("transaction_id").orderBy(desc("_ingested_at"))

df_deduped = df_bronze \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter("row_num = 1") \
    .drop("row_num")

print(f"📊 Después de dedup: {df_deduped.count()}")
print(f"✅ Duplicados removidos: {df_bronze.count() - df_deduped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Estandarización de Datos

# COMMAND ----------

df_standardized = df_deduped \
    .withColumn("product_name", upper(trim(col("product_name")))) \
    .withColumn("region", upper(trim(col("region")))) \
    .withColumn("status", lower(trim(col("status")))) \
    .withColumn("date", to_date(col("date"))) \
    .withColumn("amount", col("amount").cast("decimal(10,2)")) \
    .withColumn("quantity", col("quantity").cast("int"))

# Agregar columnas derivadas
df_standardized = df_standardized \
    .withColumn("total_value", col("amount") * col("quantity")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date")))

display(df_standardized.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Checks

# COMMAND ----------

def validate_data_quality(df):
    """Ejecuta checks de calidad y retorna DataFrame con flags"""
    
    df_with_flags = df \
        .withColumn("is_valid", lit(True)) \
        .withColumn("quality_issues", array())
    
    # Check 1: Nulls en campos críticos
    df_with_flags = df_with_flags \
        .withColumn("has_null_amount", col("amount").isNull()) \
        .withColumn("has_null_quantity", col("quantity").isNull()) \
        .withColumn("quality_issues",
                    when(col("has_null_amount") | col("has_null_quantity"),
                         array_union(col("quality_issues"), array(lit("null_values"))))
                    .otherwise(col("quality_issues")))
    
    # Check 2: Valores negativos
    df_with_flags = df_with_flags \
        .withColumn("has_negative", (col("amount") < 0) | (col("quantity") < 0)) \
        .withColumn("quality_issues",
                    when(col("has_negative"),
                         array_union(col("quality_issues"), array(lit("negative_values"))))
                    .otherwise(col("quality_issues")))
    
    # Check 3: Montos sospechosos (> $5000)
    df_with_flags = df_with_flags \
        .withColumn("is_suspicious", col("amount") > 5000) \
        .withColumn("quality_issues",
                    when(col("is_suspicious"),
                         array_union(col("quality_issues"), array(lit("suspicious_amount"))))
                    .otherwise(col("quality_issues")))
    
    # Check 4: Status inválido
    valid_statuses = ["completed", "pending", "cancelled"]
    df_with_flags = df_with_flags \
        .withColumn("invalid_status", ~col("status").isin(valid_statuses)) \
        .withColumn("quality_issues",
                    when(col("invalid_status"),
                         array_union(col("quality_issues"), array(lit("invalid_status"))))
                    .otherwise(col("quality_issues")))
    
    # Marcar como inválido si hay cualquier issue
    df_with_flags = df_with_flags \
        .withColumn("is_valid", size(col("quality_issues")) == 0)
    
    return df_with_flags

# COMMAND ----------

# Aplicar validaciones
df_validated = validate_data_quality(df_standardized)

# Reportar calidad
total = df_validated.count()
valid = df_validated.filter("is_valid = true").count()
invalid = total - valid

print(f"📊 Reporte de Calidad:")
print(f"  Total: {total}")
print(f"  Válidos: {valid} ({valid/total*100:.1f}%)")
print(f"  Inválidos: {invalid} ({invalid/total*100:.1f}%)")

# Ver registros con issues
if invalid > 0:
    print(f"\n⚠️  Registros con problemas:")
    df_validated.filter("is_valid = false") \
        .select("transaction_id", "amount", "quantity", "status", "quality_issues") \
        .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Guardar en Silver

# COMMAND ----------

# Solo datos válidos van a Silver
df_silver = df_validated \
    .filter("is_valid = true") \
    .select(
        "transaction_id", "date", "product_code", "product_name",
        "region", "amount", "quantity", "customer_id", "status",
        "total_value", "year", "month", "day_of_week",
        "_ingested_at", "_source", "_batch_id"
    ) \
    .withColumn("_processed_at", current_timestamp())

silver_path = "/tmp/lab04/silver/sales"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

print(f"✅ Guardado en Silver: {silver_path}")

# COMMAND ----------

# Guardar registros inválidos en tabla de errores
df_errors = df_validated \
    .filter("is_valid = false") \
    .withColumn("_error_detected_at", current_timestamp())

error_path = "/tmp/lab04/silver/sales_errors"

df_errors.write \
    .format("delta") \
    .mode("append") \
    .save(error_path)

print(f"✅ Errores guardados en: {error_path}")
```

---

## Ejercicio 3: Capa Gold - Agregaciones Business (10 min)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Gold: Métricas de Negocio

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Leer Silver
silver_path = "/tmp/lab04/silver/sales"
df_silver = spark.read.format("delta").load(silver_path)

print(f"📊 Registros Silver: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Agregación Diaria por Región

# COMMAND ----------

df_daily_region = df_silver \
    .filter("status = 'completed'") \
    .groupBy("date", "region") \
    .agg(
        count("*").alias("num_transactions"),
        sum("total_value").alias("total_sales"),
        avg("total_value").alias("avg_transaction"),
        sum("quantity").alias("total_units"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("total_sales", round(col("total_sales"), 2)) \
    .withColumn("avg_transaction", round(col("avg_transaction"), 2)) \
    .orderBy("date", "region")

print("📊 Ventas Diarias por Región:")
display(df_daily_region)

# Guardar
gold_daily_region_path = "/tmp/lab04/gold/daily_sales_by_region"
df_daily_region.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save(gold_daily_region_path)

print(f"✅ Guardado en: {gold_daily_region_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top Productos

# COMMAND ----------

df_top_products = df_silver \
    .filter("status = 'completed'") \
    .groupBy("product_code", "product_name") \
    .agg(
        sum("total_value").alias("total_revenue"),
        sum("quantity").alias("units_sold"),
        count("*").alias("num_transactions"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("total_revenue", round(col("total_revenue"), 2)) \
    .withColumn("avg_revenue_per_unit", round(col("total_revenue") / col("units_sold"), 2)) \
    .orderBy(desc("total_revenue"))

print("📊 Top 10 Productos por Revenue:")
display(df_top_products.limit(10))

# Guardar
gold_products_path = "/tmp/lab04/gold/product_performance"
df_top_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_products_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análisis de Clientes (RFM simplificado)

# COMMAND ----------

# Calcular métricas RFM
current_date = df_silver.agg(max("date")).collect()[0][0]

df_customer_rfm = df_silver \
    .filter("status = 'completed'") \
    .groupBy("customer_id") \
    .agg(
        max("date").alias("last_purchase_date"),
        count("*").alias("frequency"),
        sum("total_value").alias("monetary")
    ) \
    .withColumn("recency_days", datediff(lit(current_date), col("last_purchase_date"))) \
    .withColumn("monetary", round(col("monetary"), 2))

# Clasificar clientes
df_customer_rfm = df_customer_rfm \
    .withColumn("customer_segment",
                when((col("recency_days") < 7) & (col("frequency") >= 5) & (col("monetary") > 1000), "VIP")
                .when((col("recency_days") < 14) & (col("frequency") >= 3), "Active")
                .when((col("recency_days") < 30) & (col("monetary") > 500), "Potential")
                .otherwise("At Risk"))

print("📊 Segmentación de Clientes:")
df_customer_rfm.groupBy("customer_segment").count().orderBy(desc("count")).show()

# Guardar
gold_customers_path = "/tmp/lab04/gold/customer_segments"
df_customer_rfm.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_customers_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detección de Anomalías

# COMMAND ----------

# Calcular estadísticas por región
stats_window = Window.partitionBy("region")

df_with_stats = df_silver \
    .filter("status = 'completed'") \
    .withColumn("region_avg", avg("total_value").over(stats_window)) \
    .withColumn("region_stddev", stddev("total_value").over(stats_window))

# Identificar anomalías (z-score > 3)
df_anomalies = df_with_stats \
    .withColumn("z_score", (col("total_value") - col("region_avg")) / col("region_stddev")) \
    .filter("abs(z_score) > 3") \
    .select("transaction_id", "date", "region", "product_name", "total_value", 
            "region_avg", "z_score") \
    .orderBy(desc("z_score"))

print(f"⚠️  Transacciones Anómalas: {df_anomalies.count()}")
display(df_anomalies)

# Guardar
gold_anomalies_path = "/tmp/lab04/gold/anomalies"
df_anomalies.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_anomalies_path)
```

---

## Ejercicio 4: Operaciones Delta Lake Avanzadas (10 min)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake: MERGE, OPTIMIZE, Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. MERGE (Upsert)

# COMMAND ----------

from delta.tables import DeltaTable

# Crear tabla Delta si no existe
silver_path = "/tmp/lab04/silver/sales"

# Simular nuevos datos que actualizan existentes
updates_data = [
    ("TXN000001", "2026-05-22", "P123", "LAPTOP", "NORTH", 999.99, 1, "CUST1001", "completed"),
    ("TXN000002", "2026-05-22", "P456", "MOUSE", "SOUTH", 29.99, 5, "CUST1002", "completed"),
    ("TXN999999", "2026-05-22", "P789", "KEYBOARD", "EAST", 79.99, 2, "CUST9999", "completed")  # Nuevo
]

df_updates = spark.createDataFrame(updates_data, 
    ["transaction_id", "date", "product_code", "product_name", "region", 
     "amount", "quantity", "customer_id", "status"]
) \
.withColumn("date", to_date(col("date"))) \
.withColumn("total_value", col("amount") * col("quantity")) \
.withColumn("year", year(col("date"))) \
.withColumn("month", month(col("date"))) \
.withColumn("day_of_week", dayofweek(col("date"))) \
.withColumn("_ingested_at", current_timestamp()) \
.withColumn("_source", lit("sales_system")) \
.withColumn("_batch_id", lit("BATCH_UPDATE")) \
.withColumn("_processed_at", current_timestamp())

print("📊 Datos para MERGE:")
display(df_updates)

# COMMAND ----------

# Ejecutar MERGE
delta_table = DeltaTable.forPath(spark, silver_path)

delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("✅ MERGE completado")

# Verificar
df_merged = spark.read.format("delta").load(silver_path)
print(f"📊 Total después de MERGE: {df_merged.count()}")

# Ver registros actualizados/nuevos
df_merged.filter("transaction_id IN ('TXN000001', 'TXN000002', 'TXN999999')").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE y Z-ORDER

# COMMAND ----------

# OPTIMIZE compacta archivos pequeños
spark.sql(f"""
OPTIMIZE delta.`{silver_path}`
""")

print("✅ OPTIMIZE completado")

# COMMAND ----------

# Z-ORDER organiza datos por columnas frecuentemente usadas en filtros
spark.sql(f"""
OPTIMIZE delta.`{silver_path}`
ZORDER BY (region, date, product_code)
""")

print("✅ Z-ORDER completado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VACUUM (Limpiar archivos viejos)

# COMMAND ----------

# Ver archivos antes de VACUUM
print("📁 Archivos antes de VACUUM:")
dbutils.fs.ls(silver_path)

# COMMAND ----------

# VACUUM elimina archivos no referenciados > 7 días
# NOTA: Requiere desactivar check de retention (solo para demo)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql(f"""
VACUUM delta.`{silver_path}` RETAIN 0 HOURS
""")

print("✅ VACUUM completado")

# Restaurar configuración
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time Travel

# COMMAND ----------

# Ver historial de versiones
history = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`")
print("📜 Historial de Versiones:")
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# Leer versión específica
version_0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
version_latest = spark.read.format("delta").load(silver_path)

print(f"Versión 0: {version_0.count()} registros")
print(f"Versión actual: {version_latest.count()} registros")

# COMMAND ----------

# Leer por timestamp
from datetime import datetime, timedelta

# Obtener timestamp de versión anterior
ts = history.select("timestamp").collect()[1][0]

df_as_of = spark.read.format("delta") \
    .option("timestampAsOf", ts.strftime("%Y-%m-%d %H:%M:%S")) \
    .load(silver_path)

print(f"📅 Datos al {ts}: {df_as_of.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Restaurar Versión Anterior (Rollback)

# COMMAND ----------

# Si cometimos un error, podemos restaurar
# spark.sql(f"""
# RESTORE TABLE delta.`{silver_path}` TO VERSION AS OF 0
# """)
# 
# print("✅ Tabla restaurada a versión 0")

# (Comentado para no afectar el lab, pero funciona)
```

---

## 🎯 Desafío Final: Pipeline Completo

Implementa un pipeline end-to-end que:

1. **Bronze**: Ingesta datos desde CSV
   ```python
   # Crea CSV con datos de prueba en /tmp/source/sales.csv
   # Lee e ingesta a Bronze con metadata
   ```

2. **Silver**: Aplica todas las transformaciones
   - Deduplicación
   - Estandarización
   - Validaciones con 5+ reglas custom
   - Manejo de errores

3. **Gold**: Crea 3 agregaciones
   - Ventas por categoría temporal (hora del día)
   - Análisis de tendencias (week-over-week)
   - Cohortes de clientes (por mes de primera compra)

4. **Optimización**:
   - MERGE incremental (solo nuevos/modificados)
   - OPTIMIZE + Z-ORDER
   - Partition por fecha en Gold
   - Time travel: Comparar versiones

**Bonus**: Crea un dashboard SQL con queries sobre las tablas Gold

---

## ✅ Checklist de Completado

- ☐ Implementada capa Bronze con metadata
- ☐ Implementada capa Silver con deduplicación y validaciones
- ☐ Implementada capa Gold con agregaciones business
- ☐ Ejecutado MERGE upsert correctamente
- ☐ Optimizado tablas con OPTIMIZE y Z-ORDER
- ☐ Usado time travel para ver versiones históricas
- ☐ Completado pipeline completo del desafío

---

## 📚 Conceptos Clave

| Concepto | Propósito |
|----------|-----------|
| **Bronze** | Datos raw sin transformaciones |
| **Silver** | Datos limpios, deduplicados, validados |
| **Gold** | Agregaciones listas para consumo |
| **MERGE** | Upsert (insert + update) |
| **OPTIMIZE** | Compactar archivos para performance |
| **Z-ORDER** | Co-localizar datos relacionados |
| **VACUUM** | Eliminar archivos viejos |
| **Time Travel** | Acceder versiones históricas |

---

**Siguiente**: [Laboratorio 5 - Configuración de Jobs](./lab-05-jobs.md)
