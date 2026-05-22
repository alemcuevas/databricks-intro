# Laboratorio 5: Configuración y Ejecución de Jobs

⏱️ **Duración estimada**: 35-45 minutos  
🎯 **Nivel**: Intermedio

## 📋 Objetivos

- Crear Jobs desde notebooks
- Configurar job clusters optimizados
- Implementar multi-task workflows con dependencias
- Configurar alertas y monitoreo
- Troubleshooting de jobs fallidos
- Usar parámetros y scheduling

---

## Ejercicio 1: Crear Job Simple (10 min)

### Paso 1: Preparar Notebook para Job

Notebook: "Job_Simple_ETL"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Job Simple: ETL Diario

# COMMAND ----------

# Widgets para parametrización
dbutils.widgets.text("process_date", "2026-05-22", "📅 Fecha de Proceso")
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "🌍 Environment")
dbutils.widgets.text("batch_size", "1000", "📦 Batch Size")

# COMMAND ----------

# Importar librerías
from pyspark.sql.functions import *
from datetime import datetime
import json

# Leer parámetros
process_date = dbutils.widgets.get("process_date")
env = dbutils.widgets.get("env")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"🚀 Iniciando Job")
print(f"  Fecha: {process_date}")
print(f"  Env: {env}")
print(f"  Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract

# COMMAND ----------

try:
    # Simular lectura de datos
    df_source = spark.range(0, batch_size) \
        .withColumn("date", lit(process_date)) \
        .withColumn("value", rand() * 1000) \
        .withColumn("category", (rand() * 5).cast("int")) \
        .withColumn("extracted_at", current_timestamp())
    
    extract_count = df_source.count()
    print(f"✅ Extract: {extract_count} registros")
    
except Exception as e:
    print(f"❌ Error en Extract: {str(e)}")
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "stage": "extract", "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform

# COMMAND ----------

try:
    df_transformed = df_source \
        .withColumn("value_rounded", round(col("value"), 2)) \
        .withColumn("value_category",
                    when(col("value") < 250, "Low")
                    .when(col("value") < 750, "Medium")
                    .otherwise("High")) \
        .withColumn("processed_at", current_timestamp())
    
    transform_count = df_transformed.count()
    print(f"✅ Transform: {transform_count} registros")
    
except Exception as e:
    print(f"❌ Error en Transform: {str(e)}")
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "stage": "transform", "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load

# COMMAND ----------

try:
    output_path = f"/tmp/jobs/simple_etl/{env}/{process_date}"
    
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)
    
    load_count = df_transformed.count()
    print(f"✅ Load: {load_count} registros a {output_path}")
    
except Exception as e:
    print(f"❌ Error en Load: {str(e)}")
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "stage": "load", "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resultado

# COMMAND ----------

# Preparar resultado para retornar
result = {
    "status": "SUCCESS",
    "process_date": process_date,
    "environment": env,
    "records_extracted": extract_count,
    "records_transformed": transform_count,
    "records_loaded": load_count,
    "output_path": output_path,
    "timestamp": datetime.utcnow().isoformat()
}

print("\n📊 Resultado Final:")
print(json.dumps(result, indent=2))

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
```

### Paso 2: Crear Job desde UI

1. **Workflows → Create Job**

2. **Configuración básica**:
   ```
   Job name: Lab05_Simple_ETL
   Task name: etl_task
   Type: Notebook
   Notebook path: [seleccionar Job_Simple_ETL]
   ```

3. **Cluster configuration**:
   ```
   Cluster mode: Job cluster
   Worker type: Standard_DS3_v2
   Workers: 2-4 (autoscaling)
   Databricks Runtime: 13.3 LTS
   ```

4. **Parameters** (en Advanced → Parameters):
   ```
   process_date: 2026-05-22
   env: dev
   batch_size: 500
   ```

5. **Schedule**: Dejar sin schedule por ahora

6. **Save** y **Run now**

### Paso 3: Monitorear Ejecución

1. Ver progreso en tiempo real
2. Click en "View logs" para ver output
3. Verificar que retorne status SUCCESS
4. Revisar métricas de duración

---

## Ejercicio 2: Multi-Task Workflow con Dependencias (15 min)

### Paso 1: Crear Notebooks para cada Task

**Notebook 1**: "Job_Task1_Ingest"

```python
# Databricks notebook source
dbutils.widgets.text("date", "2026-05-22")
dbutils.widgets.text("source", "sales")

date = dbutils.widgets.get("date")
source = dbutils.widgets.get("source")

print(f"📥 Ingesting {source} data for {date}")

# Simular ingesta
from pyspark.sql.functions import *
import json

df = spark.range(0, 1000) \
    .withColumn("date", lit(date)) \
    .withColumn("source", lit(source)) \
    .withColumn("value", rand() * 1000)

output = f"/tmp/jobs/multitask/{date}/bronze/{source}"
df.write.format("delta").mode("overwrite").save(output)

result = {"status": "SUCCESS", "output": output, "count": df.count()}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
```

**Notebook 2**: "Job_Task2_Transform"

```python
# Databricks notebook source
dbutils.widgets.text("date", "2026-05-22")
dbutils.widgets.text("input_path", "")

date = dbutils.widgets.get("date")
input_path = dbutils.widgets.get("input_path") or f"/tmp/jobs/multitask/{date}/bronze/sales"

print(f"🔄 Transforming data from {input_path}")

from pyspark.sql.functions import *
import json

# Leer Bronze
df_bronze = spark.read.format("delta").load(input_path)

# Transformar
df_silver = df_bronze \
    .withColumn("value_rounded", round(col("value"), 2)) \
    .withColumn("category",
                when(col("value") < 333, "A")
                .when(col("value") < 666, "B")
                .otherwise("C")) \
    .withColumn("processed_at", current_timestamp())

# Guardar Silver
output = f"/tmp/jobs/multitask/{date}/silver/sales"
df_silver.write.format("delta").mode("overwrite").save(output)

result = {"status": "SUCCESS", "output": output, "count": df_silver.count()}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
```

**Notebook 3**: "Job_Task3_Aggregate"

```python
# Databricks notebook source
dbutils.widgets.text("date", "2026-05-22")

date = dbutils.widgets.get("date")

print(f"📊 Aggregating data for {date}")

from pyspark.sql.functions import *
import json

# Leer Silver
input_path = f"/tmp/jobs/multitask/{date}/silver/sales"
df_silver = spark.read.format("delta").load(input_path)

# Agregar
df_gold = df_silver \
    .groupBy("date", "category") \
    .agg(
        count("*").alias("count"),
        sum("value_rounded").alias("total"),
        avg("value_rounded").alias("average")
    ) \
    .withColumn("total", round(col("total"), 2)) \
    .withColumn("average", round(col("average"), 2))

# Guardar Gold
output = f"/tmp/jobs/multitask/{date}/gold/summary"
df_gold.write.format("delta").mode("overwrite").save(output)

print("📋 Summary:")
df_gold.show()

result = {"status": "SUCCESS", "output": output, "count": df_gold.count()}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
```

### Paso 2: Crear Multi-Task Job

1. **Workflows → Create Job**

2. **Job name**: `Lab05_MultiTask_Pipeline`

3. **Crear Task 1 (Ingest)**:
   ```
   Task name: ingest
   Type: Notebook
   Notebook: Job_Task1_Ingest
   Cluster: [New Job Cluster]
     - Workers: 2-4
     - Runtime: 13.3 LTS
   Parameters:
     date: 2026-05-22
     source: sales
   ```

4. **Crear Task 2 (Transform)** con dependencia:
   ```
   Task name: transform
   Type: Notebook
   Notebook: Job_Task2_Transform
   Depends on: ingest
   Cluster: [Same as Task 1]
   Parameters:
     date: 2026-05-22
   ```

5. **Crear Task 3 (Aggregate)** con dependencia:
   ```
   Task name: aggregate
   Type: Notebook
   Notebook: Job_Task3_Aggregate
   Depends on: transform
   Cluster: [Same as Task 1]
   Parameters:
     date: 2026-05-22
   ```

6. **Vista de DAG**:
   ```
   ingest → transform → aggregate
   ```

7. **Save** y **Run now**

### Paso 3: Agregar Task Paralela

Crea **Task 1B** (Ingest de otra fuente) paralela a Task 1:

**Notebook**: "Job_Task1B_Ingest_Inventory"

```python
# Databricks notebook source
dbutils.widgets.text("date", "2026-05-22")
date = dbutils.widgets.get("date")

print(f"📥 Ingesting inventory data for {date}")

from pyspark.sql.functions import *
import json

df = spark.range(0, 500) \
    .withColumn("date", lit(date)) \
    .withColumn("source", lit("inventory")) \
    .withColumn("stock", (rand() * 100).cast("int"))

output = f"/tmp/jobs/multitask/{date}/bronze/inventory"
df.write.format("delta").mode("overwrite").save(output)

result = {"status": "SUCCESS", "output": output, "count": df.count()}
dbutils.notebook.exit(json.dumps(result))
```

**Modificar Job**:
- Add Task `ingest_inventory` (usa notebook anterior)
- `transform` ahora depende de AMBOS: `ingest` Y `ingest_inventory`

**Nuevo DAG**:
```
ingest          ↘
                 → transform → aggregate
ingest_inventory ↗
```

---

## Ejercicio 3: Configurar Alertas y Retry (10 min)

### Paso 1: Configurar Retry Strategy

En el Job `Lab05_MultiTask_Pipeline`:

1. **Edit Job**
2. **Para cada task**, en Advanced:
   ```
   Max retries: 2
   Min retry interval: 60 seconds
   Retry on timeout: Yes
   Timeout: 900 seconds (15 min)
   ```

### Paso 2: Configurar Alertas

1. **Job settings → Alerts**:

   **On Success**:
   ```
   Email: tu-email@example.com
   Subject: ✅ Job completado: {{job_name}}
   ```

   **On Failure**:
   ```
   Email: tu-email@example.com
   Subject: ❌ Job fallido: {{job_name}}
   Webhook (opcional): https://hooks.slack.com/services/YOUR/WEBHOOK
   ```

   **On Duration Warning**:
   ```
   Duration threshold: 10 minutes
   Email: tu-email@example.com
   Subject: ⚠️ Job lento: {{job_name}}
   ```

### Paso 3: Simular Fallo y Retry

Modifica temporalmente `Job_Task2_Transform` para fallar:

```python
# COMMAND ----------

# Simular error (descomentar para probar retry)
# if True:
#     raise Exception("Simulated failure for testing retry")

# (resto del código...)
```

Descomenta, guarda, y ejecuta el job:
- Verás task "transform" fallar
- Automáticamente reintentará después de 60s
- Si falla 2 veces más, marcará el job como fallido
- Recibirás alerta por email

Después vuelve a comentar para que funcione.

---

## Ejercicio 4: Scheduling con Cron (5 min)

### Paso 1: Configurar Schedule

En `Lab05_MultiTask_Pipeline`:

1. **Edit Job → Schedule**
2. **Schedule type**: `Scheduled`

**Ejemplos de Schedules**:

```
# Diario a las 2 AM
0 2 * * *

# Cada hora
0 * * * *

# Lunes a Viernes a las 8 AM
0 8 * * 1-5

# Cada 15 minutos
*/15 * * * *

# Último día del mes a las 11 PM
0 23 L * *
```

3. **Para este lab, configura**:
   ```
   0 */2 * * *  (cada 2 horas)
   ```

4. **Timezone**: `UTC` (o tu timezone preferida)

5. **Pause status**: `Paused` (para no ejecutar realmente cada 2 horas)

6. **Save**

### Paso 2: Ver Próximas Ejecuciones

En la pantalla del job:
- Ver "Next run" timestamp
- Ver "Schedule" configurado
- Historial de ejecuciones pasadas

---

## Ejercicio 5: Troubleshooting de Jobs (10 min)

### Paso 1: Crear Notebook con Errores Comunes

Notebook: "Job_Problematic"

```python
# Databricks notebook source
import random

# Widgets
dbutils.widgets.dropdown("error_type", "none", 
    ["none", "memory", "key_error", "timeout", "data_quality"])

error_type = dbutils.widgets.get("error_type")

# COMMAND ----------

print(f"Ejecutando con error_type: {error_type}")

# COMMAND ----------

# Error 1: OutOfMemoryError
if error_type == "memory":
    print("⚠️  Simulando OOM...")
    # DataFrame muy grande sin optimización
    df = spark.range(0, 100000000).repartition(1000)  # Mal particionado
    df_collected = df.collect()  # ❌ NUNCA hacer collect() de datos grandes
    print(df_collected)

# COMMAND ----------

# Error 2: KeyError
if error_type == "key_error":
    print("⚠️  Simulando KeyError...")
    from pyspark.sql.functions import col
    
    df = spark.range(0, 100).toDF("id")
    # Intentar acceder columna que no existe
    df_wrong = df.select(col("nonexistent_column"))  # ❌ Columna no existe
    df_wrong.show()

# COMMAND ----------

# Error 3: Timeout (job lento)
if error_type == "timeout":
    print("⚠️  Simulando operación lenta...")
    import time
    
    for i in range(100):
        # Operación extremadamente lenta
        df = spark.range(0, 10000000).repartition(1000)
        df.write.format("delta").mode("overwrite").save(f"/tmp/slow_{i}")
        time.sleep(5)

# COMMAND ----------

# Error 4: Data Quality Issue
if error_type == "data_quality":
    print("⚠️  Simulando data quality issue...")
    from pyspark.sql.functions import col
    
    df = spark.range(0, 100).toDF("id") \
        .withColumn("value", col("id") * 10)
    
    # Validación estricta
    max_value = df.agg({"value": "max"}).collect()[0][0]
    
    if max_value > 500:  # Threshold arbitrario
        raise ValueError(f"Data quality check failed: max value {max_value} exceeds threshold 500")

# COMMAND ----------

print("✅ Job completado sin errores")
```

### Paso 2: Debugging con Spark UI

1. **Ejecuta el job** con `error_type = memory`
2. **Mientras corre**, click en el cluster
3. **Spark UI → Stages**:
   - Ver stages activos
   - Identificar stage lento
   - Ver "Tasks" → distribución desigual indica skew
4. **Executors**:
   - Ver uso de memoria por executor
   - Identificar executors con OOM

### Paso 3: Analizar Logs

Para cada error type:

1. **Run job** con cada tipo de error
2. **View logs** en task fallida
3. **Identificar**:
   - Stack trace exacto
   - Línea de código problemática
   - Mensaje de error

**Ejemplos de errores y soluciones**:

| Error | Causa | Solución |
|-------|-------|----------|
| `OutOfMemoryError` | `collect()` en DF grande | Usar `take(n)` o evitar collect |
| `AnalysisException: Column 'X' not found` | Columna no existe | Verificar schema con `df.printSchema()` |
| Job timeout | Operación lenta sin optimize | Agregar `repartition()`, `cache()`, tuning |
| `ValueError` en validación | Data quality check | Ajustar threshold o filtrar datos |

---

## 🎯 Desafío Final: Pipeline de Producción

Crea un pipeline completo con:

### Requisitos:

1. **4 Tasks en secuencia**:
   ```
   validate_input → ingest → transform → publish
   ```

2. **validate_input**: Check que fuente existe y tiene datos
   - Si falla, job entero para (no continuar)

3. **ingest**: Lee desde múltiples fuentes
   - Bronze layer con metadata

4. **transform**: Silver + Gold
   - Data quality checks
   - Si >10% datos inválidos, fallar

5. **publish**: Escribe a location final
   - Optimiza con OPTIMIZE + Z-ORDER

6. **Job Configuration**:
   - Job cluster con Spot instances
   - Retry: 2 intentos
   - Timeout: 30 min
   - Alertas configuradas
   - Schedule: Diario 3 AM

7. **Parámetros**:
   - `process_date` (debe usar fecha dinámica: yesterday)
   - `environment` (dev/prod)
   - `force_reprocess` (boolean)

8. **Métricas**: Cada task retorna
   - Status
   - Records processed
   - Duration
   - Data quality score

---

## ✅ Checklist de Completado

- ☐ Creado job simple con notebook parametrizado
- ☐ Implementado multi-task workflow con dependencias
- ☐ Configurado tasks paralelas
- ☐ Configurado retry strategy y timeouts
- ☐ Configurado alertas (email/webhook)
- ☐ Configurado scheduling con cron
- ☐ Practicado troubleshooting con Spark UI
- ☐ Analizado logs de diferentes tipos de errores
- ☐ Completado pipeline de producción

---

## 📚 Mejores Prácticas

✅ **DO**:
- Usar job clusters (50% más baratos)
- Configurar timeouts razonables
- Implementar retry con backoff
- Retornar resultados con `dbutils.notebook.exit()`
- Usar parámetros para flexibilidad
- Loggear métricas importantes
- Configurar alertas

❌ **DON'T**:
- Usar all-purpose clusters para jobs
- Omitir manejo de errores
- Hacer `collect()` de datos grandes
- Hardcodear valores (usar params)
- Ignorar optimizaciones (OPTIMIZE, partition)

---

**Siguiente**: [Laboratorio 6 - Integración con Azure](./lab-06-integracion-datos.md)
