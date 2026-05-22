# Laboratorio 6: Integración y Procesamiento de Datos

⏱️ **Duración estimada**: 40-45 minutos  
🎯 **Nivel**: Intermedio

## 📋 Objetivos

- Trabajar con múltiples formatos de datos (CSV, JSON, Parquet)
- Integrar datos de diferentes fuentes
- Implementar joins complejos
- Usar Auto Loader para streaming
- Crear una capa Serving optimizada
- Best practices de almacenamiento

---

## 📦 Pre-requisitos

- Cluster de Databricks activo
- Acceso a DBFS (/tmp/)
- Notebook: `notebooks/lab-06-integracion.ipynb`

---

## Ejercicio 1: Preparar Datos de Diferentes Fuentes (10 min)

### Paso 1: Generar Datos en CSV (Catálogo de Productos)

Notebook: **Lab06_MultiFormat**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Integración de Múltiples Formatos

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Generar catálogo de productos
products_data = [
    {"product_id": f"P{i:03d}", 
     "name": f"Product {i}", 
     "category": random.choice(["Electronics", "Clothing", "Food", "Books"]), 
     "price": round(random.uniform(10, 500), 2)}
    for i in range(1, 51)
]

df_products = spark.createDataFrame(products_data)

# Guardar como CSV
products_path = "/tmp/lab06/source/products.csv"
df_products.write.format("csv").option("header", "true").mode("overwrite").save(products_path)

print(f"✅ Productos guardados en CSV: {products_path}")
display(df_products.limit(10))
```

### Paso 2: Generar Datos en JSON (Transacciones)

```python
# COMMAND ----------

# Generar transacciones
transactions_data = []
for i in range(200):
    transactions_data.append({
        "transaction_id": f"TXN{i:05d}",
        "timestamp": (datetime.now() - timedelta(hours=random.randint(0, 72))).isoformat(),
        "product_id": f"P{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 5),
        "customer_id": f"C{random.randint(1000, 9999)}"
    })

df_transactions = spark.createDataFrame(transactions_data)

# Guardar como JSON
transactions_path = "/tmp/lab06/source/transactions.json"
df_transactions.write.format("json").mode("overwrite").save(transactions_path)

print(f"✅ Transacciones guardadas en JSON: {transactions_path}")
display(df_transactions.limit(10))
```

### Paso 3: Generar Datos en Parquet (Clientes)

```python
# COMMAND ----------

# Generar datos de clientes
customers_data = [
    {
        "customer_id": f"C{i}",
        "name": f"Customer {i}",
        "email": f"customer{i}@example.com",
        "country": random.choice(["US", "UK", "CA", "AU", "MX"])
    }
    for i in range(1000, 10000)
]

df_customers = spark.createDataFrame(customers_data)

# Guardar como Parquet
customers_path = "/tmp/lab06/source/customers.parquet"
df_customers.write.format("parquet").mode("overwrite").save(customers_path)

print(f"✅ Clientes guardados en Parquet: {customers_path}")
display(df_customers.limit(10))
```

---

## Ejercicio 2: Leer y Unificar Datos (10 min)

### Paso 1: Leer Cada Formato

```python
# COMMAND ----------
# MAGIC %md
# MAGIC ## Lectura de Diferentes Formatos

# COMMAND ----------

# Leer CSV
df_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(products_path)

print(f"📊 Productos (CSV): {df_products.count()} registros")
display(df_products.limit(5))

# COMMAND ----------

# Leer JSON
df_transactions = spark.read.format("json").load(transactions_path)

print(f"📊 Transacciones (JSON): {df_transactions.count()} registros")
display(df_transactions.limit(5))

# COMMAND ----------

# Leer Parquet
df_customers = spark.read.format("parquet").load(customers_path)

print(f"📊 Clientes (Parquet): {df_customers.count()} registros")
display(df_customers.limit(5))
```

### Paso 2: Join y Enriquecimiento

```python
# COMMAND ----------
# MAGIC %md
# MAGIC ## Joins Entre Datasets

# COMMAND ----------

# Join 1: Transacciones + Productos
df_enriched = df_transactions \
    .join(df_products, "product_id", "left") \
    .withColumn("total_amount", col("quantity") * col("price"))

print(f"✅ Join Transacciones + Productos: {df_enriched.count()} registros")
display(df_enriched.limit(10))

# COMMAND ----------

# Join 2: Agregar info de Clientes
df_complete = df_enriched \
    .join(df_customers, "customer_id", "left") \
    .select(
        "transaction_id",
        "timestamp",
        "customer_id",
        col("name").alias("customer_name"),
        "email",
        "country",
        "product_id",
        df_products["name"].alias("product_name"),
        "category",
        "quantity",
        "price",
        "total_amount"
    )

print(f"✅ Dataset Completo: {df_complete.count()} registros")
display(df_complete.limit(10))
```

---

## Ejercicio 3: Agregaciones y Análisis (10 min)

### Paso 1: Análisis por Categoría

```python
# COMMAND ----------
# MAGIC %md
# MAGIC ## Análisis de Negocio

# COMMAND ----------

# Agregación por categoría
df_by_category = df_complete \
    .groupBy("category") \
    .agg(
        count("*").alias("num_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_amount"),
        sum("quantity").alias("total_units_sold"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .orderBy(col("total_revenue").desc())

print("📊 Análisis por Categoría:")
display(df_by_category)
```

### Paso 2: Análisis Geográfico

```python
# COMMAND ----------

# Agregación por país
df_by_country = df_complete \
    .groupBy("country") \
    .agg(
        count("*").alias("num_transactions"),
        sum("total_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products")
    ) \
    .orderBy(col("total_revenue").desc())

print("📊 Análisis por País:")
display(df_by_country)
```

### Paso 3: Top Productos

```python
# COMMAND ----------

# Top 10 productos más vendidos
df_top_products = df_complete \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        sum("quantity").alias("total_quantity"),
        sum("total_amount").alias("total_revenue"),
        count("*").alias("num_orders")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)

print("📊 Top 10 Productos:")
display(df_top_products)
```

---

## Ejercicio 4: Guardar en Data Lakehouse (5 min)

### Paso 1: Capa de Detalles (Particionada)

```python
# COMMAND ----------
# MAGIC %md
# MAGIC ## Guardar en Lakehouse

# COMMAND ----------

# Agregar columna de fecha para particionar
df_with_date = df_complete.withColumn("date", to_date("timestamp"))

# Guardar particionado por categoría y país
lakehouse_path = "/tmp/lab06/lakehouse/transactions_complete"

df_with_date.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("category", "country") \
    .save(lakehouse_path)

print(f"✅ Datos guardados en: {lakehouse_path}")

# COMMAND ----------

# Verificar particiones
files = dbutils.fs.ls(lakehouse_path)
partitions = [f.name for f in files if f.name.startswith("category=")]
print(f"📁 Particiones creadas: {len(partitions)}")
for p in partitions[:5]:
    print(f"  - {p}")
```

### Paso 2: Capa Serving (Agregaciones Pre-calculadas)

```python
# COMMAND ----------

# Guardar agregaciones para consultas rápidas
serving_path = "/tmp/lab06/serving"

# 1. Por categoría
df_by_category.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{serving_path}/by_category")

# 2. Por país
df_by_country.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{serving_path}/by_country")

# 3. Top productos
df_top_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{serving_path}/top_products")

print(f"✅ Capa Serving creada en: {serving_path}")
```

---

## Ejercicio 5: Streaming con Auto Loader (10 min)

### Paso 1: Preparar Directorio de Streaming

```python
# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming con Auto Loader

# COMMAND ----------

# Paths para streaming
streaming_input = "/tmp/lab06/streaming/input"
streaming_checkpoint = "/tmp/lab06/streaming/checkpoint"
streaming_output = "/tmp/lab06/streaming/output"

# Limpiar ejecuciones anteriores
dbutils.fs.rm(streaming_input, True)
dbutils.fs.rm(streaming_checkpoint, True)
dbutils.fs.rm(streaming_output, True)

print("✅ Directorios de streaming limpiados")

# COMMAND ----------

# Crear archivo inicial
batch1 = spark.range(0, 100).withColumn("value", rand() * 1000)
batch1.write.format("json").mode("overwrite").save(f"{streaming_input}/batch1")

print(f"✅ Batch inicial creado en: {streaming_input}/batch1")
```

### Paso 2: Configurar Auto Loader

```python
# COMMAND ----------

# Leer stream con Auto Loader (cloudFiles)
df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", streaming_checkpoint) \
    .load(streaming_input)

# Transformaciones
df_stream_processed = df_stream \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("value_category",
                when(col("value") < 250, "Low")
                .when(col("value") < 750, "Medium")
                .otherwise("High"))

print("✅ Stream configurado")
```

### Paso 3: Iniciar Streaming

```python
# COMMAND ----------

# Escribir stream a Delta
query = df_stream_processed.writeStream \
    .format("delta") \
    .option("checkpointLocation", streaming_checkpoint) \
    .outputMode("append") \
    .start(streaming_output)

print("🔄 Streaming iniciado...")
print(f"   Query ID: {query.id}")
print(f"   Status: {query.status}")
```

### Paso 4: Agregar Nuevos Lotes

```python
# COMMAND ----------

import time

# Agregar más batches (simula ingesta continua)
for i in range(2, 5):
    batch = spark.range(0, 50).withColumn("value", rand() * 1000)
    batch.write.format("json").mode("overwrite").save(f"{streaming_input}/batch{i}")
    print(f"✅ Batch {i} agregado")
    time.sleep(2)

print("\n⏳ Esperando procesamiento...")
time.sleep(5)
```

### Paso 5: Verificar Resultados

```python
# COMMAND ----------

# Leer datos procesados
df_stream_results = spark.read.format("delta").load(streaming_output)
print(f"📊 Total registros procesados: {df_stream_results.count()}")

# Ver distribución
display(df_stream_results.groupBy("value_category").count().orderBy("value_category"))

# COMMAND ----------

# Detener streaming
query.stop()
print("⏹️  Streaming detenido")
```

---

## 🎯 Resumen del Lab

Has completado:

✅ **Formato de Datos**: CSV, JSON, Parquet  
✅ **Joins Complejos**: 3 datasets combinados  
✅ **Agregaciones**: Por categoría, país, producto  
✅ **Lakehouse**: Particionado por múltiples columnas  
✅ **Serving Layer**: Pre-agregaciones optimizadas  
✅ **Streaming**: Auto Loader con checkpointing  

---

## 📚 Best Practices Implementadas

### 1. **Selección de Formato**
```
CSV      → Intercambio con sistemas externos
JSON     → APIs y datos semi-estructurados
Parquet  → Almacenamiento columnar eficiente
Delta    → Lakehouse con ACID y time travel
```

### 2. **Particionado Estratégico**
```python
# ✅ BUENO: Cardinalidad media, queries frecuentes
.partitionBy("category", "country")

# ❌ MALO: Demasiadas particiones (alta cardinalidad)
.partitionBy("transaction_id")

# ❌ MALO: Muy pocas particiones
.partitionBy("year")
```

### 3. **Capa Serving**
- Pre-calcular agregaciones comunes
- Actualizar en batch (diario/hora)
- Queries rápidas para dashboards

### 4. **Streaming Patterns**
```python
# Auto Loader: Schema evolution automático
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", checkpoint_path)

# Checkpointing: Exactamente-una-vez
.option("checkpointLocation", checkpoint_path)
```

---

## 🚀 Próximos Pasos

### Para Producción:

1. **Conectar con Servicios Azure Reales**:
   - Azure Storage (ADLS Gen2)
   - Azure Cosmos DB
   - Azure SQL Database
   - Azure Event Hubs

2. **Implementar Seguridad**:
   - Service Principal + OAuth
   - Azure Key Vault para secretos
   - RBAC en Storage

3. **Optimización**:
   - OPTIMIZE + Z-ORDER en Delta
   - Vacuum para cleanup
   - Caching para queries frecuentes

4. **Monitoreo**:
   - Métricas de streaming
   - Data quality checks
   - Alertas en failures

---

## ✅ Checklist de Validación

- [ ] Puedes leer CSV, JSON y Parquet
- [ ] Joins funcionan correctamente
- [ ] Agregaciones producen resultados esperados
- [ ] Datos se guardan particionados
- [ ] Streaming procesa archivos nuevos automáticamente
- [ ] Checkpointing permite recovery
- [ ] Capa Serving tiene agregaciones pre-calculadas

---

## 📖 Referencias

- [Auto Loader Docs](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Partitioning Strategies](https://docs.databricks.com/delta/partition.html)
