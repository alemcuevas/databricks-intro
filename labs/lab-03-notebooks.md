# Laboratorio 3: Notebooks Avanzados

⏱️ **Duración estimada**: 35-45 minutos  
🎯 **Nivel**: Intermedio

## 📋 Objetivos

- Dominar magic commands (%sql, %run, %fs)
- Usar widgets para parametrización
- Comunicación entre notebooks
- Debugging y logging efectivo
- Mejores prácticas de documentación

---

## Ejercicio 1: Magic Commands (10 min)

### Paso 1: Crear Notebook Multi-lenguaje

Crea notebook: "Lab03_Magic_Commands"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Laboratorio 3: Magic Commands
# MAGIC 
# MAGIC Exploraremos:
# MAGIC - Markdown para documentación
# MAGIC - SQL para queries
# MAGIC - Shell commands
# MAGIC - File system operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creación de Datos de Prueba

# COMMAND ----------

# Python: Crear DataFrame
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

# Generar datos de ventas
dates = [(datetime.now() - timedelta(days=x)).strftime("%Y-%m-%d") for x in range(30)]
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Webcam"]
regions = ["North", "South", "East", "West"]

data = []
for _ in range(1000):
    data.append((
        random.choice(dates),
        random.choice(products),
        random.choice(regions),
        random.uniform(50, 500),
        random.randint(1, 5)
    ))

df = spark.createDataFrame(data, ["date", "product", "region", "amount", "quantity"])

# Guardar como tabla temporal
df.createOrReplaceTempView("sales")

print(f"✅ Creados {df.count()} registros de ventas")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Usar %sql para Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query SQL directo
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   product,
# MAGIC   COUNT(*) as transactions,
# MAGIC   ROUND(SUM(amount), 2) as total_sales,
# MAGIC   ROUND(AVG(amount), 2) as avg_sale
# MAGIC FROM sales
# MAGIC GROUP BY region, product
# MAGIC ORDER BY total_sales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Combinar Python y SQL

# COMMAND ----------

# Ejecutar SQL desde Python y capturar resultado
result_df = spark.sql("""
SELECT 
  date,
  SUM(amount) as daily_total
FROM sales
GROUP BY date
ORDER BY date
""")

print(f"📊 Datos diarios: {result_df.count()} días")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. File System Operations con %fs

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# Guardar DataFrame a DBFS
output_path = "/tmp/lab03_sales"
df.write.mode("overwrite").parquet(output_path)
print(f"✅ Guardado en: {output_path}")

# COMMAND ----------

# MAGIC %fs ls /tmp/lab03_sales/

# COMMAND ----------

# Ver contenido de archivo
# MAGIC %fs head /tmp/lab03_sales/_SUCCESS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Shell Commands con %sh

# COMMAND ----------

# MAGIC %sh
# MAGIC # Ver info del sistema
# MAGIC echo "Hostname: $(hostname)"
# MAGIC echo "OS: $(cat /etc/os-release | grep PRETTY_NAME)"
# MAGIC echo "CPU cores: $(nproc)"
# MAGIC echo "Memory: $(free -h | grep Mem | awk '{print $2}')"

# COMMAND ----------

# MAGIC %sh
# MAGIC # Instalar herramienta CLI (ejemplo)
# MAGIC pip install cowsay --quiet
# MAGIC cowsay "Hello from Databricks!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resumen
# MAGIC 
# MAGIC | Magic Command | Uso |
# MAGIC |---------------|-----|
# MAGIC | `%md` | Markdown/documentación |
# MAGIC | `%sql` | Queries SQL directos |
# MAGIC | `%python` | Código Python (default) |
# MAGIC | `%scala` | Código Scala |
# MAGIC | `%r` | Código R |
# MAGIC | `%sh` | Shell commands |
# MAGIC | `%fs` | File system ops (ls, cp, rm) |
# MAGIC | `%run` | Ejecutar otro notebook |
```

---

## Ejercicio 2: Widgets y Parametrización (10 min)

### Paso 1: Crear Notebook Parametrizado

Notebook: "Lab03_Widgets"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Parametrización con Widgets

# COMMAND ----------

# Crear widgets de diferentes tipos
dbutils.widgets.text("fecha", "2026-05-22", "📅 Fecha (YYYY-MM-DD)")
dbutils.widgets.dropdown("region", "All", ["All", "North", "South", "East", "West"], "🌍 Región")
dbutils.widgets.dropdown("product", "All", ["All", "Laptop", "Mouse", "Keyboard", "Monitor", "Webcam"], "📦 Producto")
dbutils.widgets.combobox("min_amount", "100", ["50", "100", "200", "500"], "💰 Monto Mínimo")

print("✅ Widgets creados")

# COMMAND ----------

# Leer parámetros
fecha = dbutils.widgets.get("fecha")
region = dbutils.widgets.get("region")
product = dbutils.widgets.get("product")
min_amount = float(dbutils.widgets.get("min_amount"))

print(f"Parámetros:")
print(f"  Fecha: {fecha}")
print(f"  Región: {region}")
print(f"  Producto: {product}")
print(f"  Monto mínimo: ${min_amount}")

# COMMAND ----------

# Construir query dinámico basado en parámetros
where_clauses = [f"amount >= {min_amount}"]

if region != "All":
    where_clauses.append(f"region = '{region}'")

if product != "All":
    where_clauses.append(f"product = '{product}'")

where_clause = " AND ".join(where_clauses)

query = f"""
SELECT *
FROM sales
WHERE {where_clause}
"""

print(f"📝 Query generado:")
print(query)

# COMMAND ----------

# Ejecutar query
df_filtered = spark.sql(query)

print(f"\n📊 Resultados: {df_filtered.count()} registros")
display(df_filtered)

# COMMAND ----------

# Resumen
df_summary = df_filtered.groupBy("region", "product").agg(
    count("*").alias("transactions"),
    round(sum("amount"), 2).alias("total_sales"),
    round(avg("amount"), 2).alias("avg_sale")
).orderBy(desc("total_sales"))

display(df_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Prueba diferentes combinaciones
# MAGIC 
# MAGIC Usa los widgets arriba para probar:
# MAGIC - Región: North, Producto: Laptop
# MAGIC - Región: All, Monto mínimo: 300
# MAGIC - etc.
```

---

## Ejercicio 3: Comunicación entre Notebooks (10 min)

### Paso 1: Crear Notebook de Utilidades

Notebook: "Lab03_Utils"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Funciones Utilitarias

# COMMAND ----------

from pyspark.sql.functions import col, when
from datetime import datetime
import json

def validate_dataframe(df, required_columns, min_rows=1):
    """Valida estructura y contenido de DataFrame"""
    # Validar columnas
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes: {missing}")
    
    # Validar cantidad
    count = df.count()
    if count < min_rows:
        raise ValueError(f"Pocos registros: {count} < {min_rows}")
    
    return True

def add_data_quality_flags(df):
    """Agrega columnas de quality flags"""
    return df \
        .withColumn("has_nulls", 
                    when(col("amount").isNull() | col("quantity").isNull(), True).otherwise(False)) \
        .withColumn("is_valid_amount",
                    when(col("amount") > 0, True).otherwise(False))

def log_metrics(stage, metrics):
    """Log estructurado de métricas"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "stage": stage,
        **metrics
    }
    print(f"METRICS: {json.dumps(log_entry)}")

print("✅ Funciones utilitarias cargadas")
```

### Paso 2: Crear Notebook Principal que Llama Utils

Notebook: "Lab03_Main"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Principal con Reutilización

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar Utilidades

# COMMAND ----------

# Cargar funciones desde otro notebook
%run ./Lab03_Utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Procesar Datos

# COMMAND ----------

from pyspark.sql.functions import *

# Crear datos de prueba
data = [
    ("2026-05-22", "Laptop", "North", 500, 1),
    ("2026-05-22", "Mouse", "South", None, 2),  # Null amount
    ("2026-05-22", "Keyboard", "East", -50, 1),  # Negative amount
    ("2026-05-22", "Monitor", "West", 300, 1)
]

df = spark.createDataFrame(data, ["date", "product", "region", "amount", "quantity"])

print("📊 Datos originales:")
display(df)

# COMMAND ----------

# Usar función de utils para validar
try:
    validate_dataframe(df, ["date", "product", "region", "amount"], min_rows=1)
    print("✅ Validación básica passed")
except ValueError as e:
    print(f"❌ Validación failed: {e}")

# COMMAND ----------

# Agregar quality flags
df_with_flags = add_data_quality_flags(df)

print("📊 Con quality flags:")
display(df_with_flags)

# COMMAND ----------

# Filtrar solo válidos
df_valid = df_with_flags.filter(
    (col("has_nulls") == False) & 
    (col("is_valid_amount") == True)
)

print(f"\n📊 Registros válidos: {df_valid.count()}/{df.count()}")
display(df_valid)

# COMMAND ----------

# Log métricas
log_metrics("data_quality", {
    "total_records": df.count(),
    "valid_records": df_valid.count(),
    "invalid_records": df.count() - df_valid.count()
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Llamar a Otro Notebook y Recibir Resultado

# COMMAND ----------

# Simula notebook de procesamiento que retorna resultado
# (Primero necesitamos crear ese notebook)
```

### Paso 3: Crear Notebook Callable

Notebook: "Lab03_Process"

```python
# Databricks notebook source
# Recibir parámetros
dbutils.widgets.text("input_path", "/tmp/data", "Input Path")
dbutils.widgets.text("date", "2026-05-22", "Process Date")

input_path = dbutils.widgets.get("input_path")
date = dbutils.widgets.get("date")

print(f"Procesando: {input_path} para fecha {date}")

# COMMAND ----------

# Simular procesamiento
import time
import json

time.sleep(2)  # Simular trabajo

result = {
    "status": "SUCCESS",
    "records_processed": 1000,
    "date": date,
    "duration_seconds": 2
}

# COMMAND ----------

# Retornar resultado como JSON
dbutils.notebook.exit(json.dumps(result))
```

### Paso 4: Llamar desde Main

Agregar al final de "Lab03_Main":

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orquestación de Notebooks

# COMMAND ----------

import json

# Llamar a otro notebook con parámetros
result_json = dbutils.notebook.run(
    "./Lab03_Process",
    timeout_seconds=300,
    arguments={
        "input_path": "/tmp/lab03_sales",
        "date": "2026-05-22"
    }
)

# Parsear resultado
result = json.loads(result_json)

print("📊 Resultado del notebook llamado:")
print(f"  Status: {result['status']}")
print(f"  Registros: {result['records_processed']}")
print(f"  Duración: {result['duration_seconds']}s")

# COMMAND ----------

# Verificar éxito y continuar
if result["status"] == "SUCCESS":
    print("✅ Pipeline completado exitosamente")
else:
    raise Exception(f"Pipeline failed: {result}")
```

---

## Ejercicio 4: Debugging y Logging (5 min)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Debugging y Logging

# COMMAND ----------

import logging
from functools import wraps
import time

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def time_it(func):
    """Decorator para medir tiempo de ejecución"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logger.info(f"{func.__name__} tomó {duration:.2f}s")
        return result
    return wrapper

@time_it
def slow_operation():
    """Operación lenta de ejemplo"""
    df = spark.range(0, 10000000).repartition(20)
    count = df.count()
    return count

# COMMAND ----------

# Ejecutar con timing
result = slow_operation()
print(f"Resultado: {result:,}")

# COMMAND ----------

# Debugging avanzado: Inspeccionar plan de ejecución
df = spark.range(0, 1000000) \
    .filter("id > 500000") \
    .selectExpr("id", "id * 2 as doubled")

# Ver logical plan
print("📋 Logical Plan:")
df.explain(mode="simple")

print("\n📋 Physical Plan:")
df.explain(mode="extended")

# COMMAND ----------

# Encontrar columnas con nulls
from pyspark.sql.functions import col, count, when, isnan

def find_null_columns(df):
    """Identifica columnas con nulls"""
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
    
    null_dict = null_counts.collect()[0].asDict()
    columns_with_nulls = {k: v for k, v in null_dict.items() if v > 0}
    
    return columns_with_nulls

# Probar
test_df = spark.createDataFrame([
    (1, "A", 100),
    (2, None, 200),
    (3, "C", None)
], ["id", "name", "value"])

nulls = find_null_columns(test_df)
print(f"Columnas con nulls: {nulls}")
```

---

## 🎯 Desafío Final: Crear Pipeline Modular

Crea una estructura de notebooks para procesar ventas:

```
notebooks/
├── Main_Pipeline.py
├── utils/
│   ├── Common_Functions.py
│   ├── Data_Quality.py
│   └── Metrics_Logger.py
├── stages/
│   ├── 01_Ingest.py
│   ├── 02_Transform.py
│   └── 03_Export.py
```

**Requisitos**:
1. Main_Pipeline orquesta las 3 stages
2. Cada stage usa funciones de utils
3. Cada stage retorna métricas (records in/out, duration)
4. Main_Pipeline agrega métricas y reporta resumen final
5. Manejo de errores en cada stage

---

## ✅ Checklist de Completado

- ☐ Dominado magic commands (%md, %sql, %fs, %sh)
- ☐ Creado notebooks con widgets parametrizados
- ☐ Implementado comunicación entre notebooks con %run
- ☐ Implementado orquestación con dbutils.notebook.run()
- ☐ Agregado logging y debugging avanzado
- ☐ Completado desafío de pipeline modular

---

**Siguiente**: [Laboratorio 4 - Transformación de Datos](./lab-04-transformacion-datos.md)
