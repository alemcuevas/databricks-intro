# Lab 02: Integración de Event Hubs con Databricks Streaming

**Duración estimada**: 45-50 minutos  
**Nivel**: Intermedio  
**Objetivo**: Consumir eventos de Event Hub en tiempo real con Databricks Structured Streaming, procesarlos y guardarlos en Delta Lake

---

## 📋 Prerequisitos

- Lab 01 completado (Event Hub con eventos)
- Workspace de Azure Databricks
- Cluster de Databricks activo (Runtime 13.3 LTS o superior)
- Acceso a Event Hub y Storage Account del Lab 01

---

## Parte 1: Configuración de Event Hub (5 min)

### Paso 1.1: Obtener Configuración de Event Hub

En Azure CLI:

```bash
# Usar mismas variables del Lab 01
NAMESPACE="<tu-namespace>"
EVENTHUB_NAME="telemetry"
RESOURCE_GROUP="rg-eventhubs-lab01"

# Obtener connection string
az eventhubs namespace authorization-rule keys list \
  --namespace-name $NAMESPACE \
  --name RootManageSharedAccessKey \
  --resource-group $RESOURCE_GROUP \
  --query primaryConnectionString -o tsv
```

### Paso 1.2: Generar Eventos Continuos

Antes de empezar el lab, ejecuta este productor que genera eventos continuamente:

Crear `continuous_producer.py`:

```python
"""Productor continuo para alimentar stream de Databricks"""
import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

CONNECTION_STRING = "<tu-connection-string>"
EVENTHUB_NAME = "telemetry"

async def generate_event(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(18.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 70.0), 2),
        "pressure": round(random.uniform(1000.0, 1020.0), 2),
        "status": random.choice(["ok", "ok", "ok", "warning"])
    }

async def continuous_send():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    
    print("🚀 Generando eventos continuamente (Ctrl+C para detener)...")
    
    async with producer:
        while True:
            batch = await producer.create_batch()
            
            # Generar 20 eventos por ciclo (4 sensores, 5 eventos cada uno)
            for sensor_num in range(1, 5):
                for _ in range(5):
                    sensor_id = f"sensor_{sensor_num:03d}"
                    event_data = await generate_event(sensor_id)
                    event = EventData(json.dumps(event_data))
                    event.partition_key = sensor_id
                    batch.add(event)
            
            await producer.send_batch(batch)
            print(f"📦 {datetime.utcnow().strftime('%H:%M:%S')} - 20 eventos enviados")
            
            await asyncio.sleep(2)  # Enviar cada 2 segundos

if __name__ == "__main__":
    try:
        asyncio.run(continuous_send())
    except KeyboardInterrupt:
        print("\n✅ Productor detenido")
```

**Ejecutar en terminal separada**:
```bash
python continuous_producer.py
```

Déjalo corriendo durante todo el lab.

---

## Parte 2: Configurar Databricks (10 min)

### Paso 2.1: Crear Notebook en Databricks

1. Navegar a Databricks Workspace
2. Crear notebook: `Lab02-EventHubs-Streaming`
3. Adjuntar a cluster activo

### Paso 2.2: Configurar Secretos (Recomendado)

**Opción 1: Usando Databricks Secrets** (recomendado para producción)

```python
# Celda 1: Configurar secretos (ejecutar una vez)
dbutils.secrets.help()

# Crear secret scope (solo si no existe)
# Se hace via Databricks CLI o Azure Key Vault
```

Para este lab, usaremos **Opción 2** (más simple):

**Opción 2: Variables en Notebook** (solo para desarrollo)

```python
# Celda 1: Configuración
# ⚠️ IMPORTANTE: No subir este notebook a GitHub con estas credenciales

# Connection string de Event Hub
eventhub_connection_string = "<tu-connection-string-completo>"

# Extraer namespace y key
# Formato: Endpoint=sb://NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=KEY
import re
match = re.search(r'Endpoint=sb://([^.]+)\.', eventhub_connection_string)
eventhub_namespace = match.group(1)

match = re.search(r'SharedAccessKey=([^;]+)', eventhub_connection_string)
eventhub_key = match.group(1)

print(f"✓ Namespace: {eventhub_namespace}")
print(f"✓ Key configurado")
```

### Paso 2.3: Configurar Conexión a Event Hub

```python
# Celda 2: Configuración de Event Hub para Spark

# Configuración de conexión
eventhub_config = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhub_connection_string),
    'eventhubs.consumerGroup': '$Default'
}

print("✓ Configuración de Event Hub lista")
```

---

## Parte 3: Leer Stream de Event Hub (10 min)

### Paso 3.1: Crear Stream Reader

```python
# Celda 3: Leer stream desde Event Hub

# Leer stream de eventos
df_raw = (spark
  .readStream
  .format("eventhubs")
  .options(**eventhub_config)
  .load()
)

# Ver schema de Event Hub
print("📋 Schema del stream de Event Hub:")
df_raw.printSchema()
```

**Salida esperada**:
```
root
 |-- body: binary (nullable = true)
 |-- partition: string (nullable = true)
 |-- offset: string (nullable = true)
 |-- sequenceNumber: long (nullable = true)
 |-- enqueuedTime: timestamp (nullable = true)
 |-- publisher: string (nullable = true)
 |-- partitionKey: string (nullable = true)
```

### Paso 3.2: Parsear Body del Evento

```python
# Celda 4: Parsear eventos JSON

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Definir schema del evento (debe coincidir con el JSON del productor)
event_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # Viene como string, convertiremos después
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Parsear body (que viene como bytes) a JSON
df_parsed = (df_raw
  # Convertir body de bytes a string
  .withColumn("body_str", col("body").cast("string"))
  
  # Parsear JSON usando el schema
  .withColumn("event", from_json(col("body_str"), event_schema))
  
  # Expandir campos del JSON
  .select(
    col("event.sensor_id").alias("sensor_id"),
    col("event.timestamp").alias("event_timestamp"),
    col("event.temperature").alias("temperature"),
    col("event.humidity").alias("humidity"),
    col("event.pressure").alias("pressure"),
    col("event.status").alias("status"),
    col("enqueuedTime").alias("enqueued_time"),  # Timestamp de Event Hub
    col("partitionKey").alias("partition_key"),
    col("offset").alias("event_offset")
  )
)

print("✓ Stream parseado correctamente")
df_parsed.printSchema()
```

---

## Parte 4: Transformaciones en Tiempo Real (10 min)

### Paso 4.1: Convertir Timestamp

```python
# Celda 5: Convertir timestamp a formato correcto

from pyspark.sql.functions import to_timestamp, current_timestamp, expr

df_transformed = (df_parsed
  # Convertir string timestamp a timestamp real
  .withColumn("event_timestamp", to_timestamp("event_timestamp"))
  
  # Calcular latencia (tiempo entre evento y llegada a Databricks)
  .withColumn("latency_seconds", 
    expr("unix_timestamp(enqueued_time) - unix_timestamp(event_timestamp)")
  )
  
  # Agregar timestamp de procesamiento
  .withColumn("processed_at", current_timestamp())
)

print("✓ Transformaciones aplicadas")
```

### Paso 4.2: Filtrar Alertas

```python
# Celda 6: Crear stream de alertas

# Stream solo con warnings (sensores con problemas)
df_warnings = (df_transformed
  .filter(col("status") == "warning")
  .select(
    "sensor_id",
    "event_timestamp",
    "temperature",
    "humidity",
    "status",
    "latency_seconds"
  )
)

print("✓ Stream de warnings creado")
```

---

## Parte 5: Escribir a Delta Lake (10 min)

### Paso 5.1: Escribir Stream Completo a Bronze Layer

```python
# Celda 7: Guardar stream completo en Delta (Bronze layer)

# Definir ubicación para Bronze layer (raw data)
bronze_path = "/tmp/lab02_bronze_telemetry"
checkpoint_bronze = "/tmp/lab02_checkpoints_bronze"

# Escribir stream a Delta Lake
query_bronze = (df_transformed
  .writeStream
  .format("delta")
  .outputMode("append")  # Agregar eventos continuamente
  .option("checkpointLocation", checkpoint_bronze)
  .start(bronze_path)
)

print(f"✓ Stream Bronze iniciado: {query_bronze.id}")
print(f"  Estado: {query_bronze.status}")
```

### Paso 5.2: Escribir Stream de Warnings a Silver Layer

```python
# Celda 8: Guardar solo warnings en Silver layer

silver_path = "/tmp/lab02_silver_warnings"
checkpoint_silver = "/tmp/lab02_checkpoints_silver"

query_silver = (df_warnings
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_silver)
  .start(silver_path)
)

print(f"✓ Stream Silver (warnings) iniciado: {query_silver.id}")
print(f"  Estado: {query_silver.status}")
```

### Paso 5.3: Verificar que Streams Están Activos

```python
# Celda 9: Ver streams activos

# Listar todos los streams activos
active_streams = spark.streams.active

print(f"📊 Streams activos: {len(active_streams)}")
for stream in active_streams:
    print(f"  - {stream.name if stream.name else stream.id}")
    print(f"    Estado: {stream.status}")
    print(f"    Última progreso: {stream.lastProgress}")
    print()
```

---

## Parte 6: Consultar Datos en Tiempo Real (5 min)

### Paso 6.1: Ver Datos del Bronze Layer

```python
# Celda 10: Leer datos de Bronze layer

# Leer Delta table
df_bronze_read = spark.read.format("delta").load(bronze_path)

print(f"📊 Total eventos en Bronze: {df_bronze_read.count()}")

# Ver últimos 10 eventos
display(df_bronze_read.orderBy(col("event_timestamp").desc()).limit(10))
```

### Paso 6.2: Ver Warnings en Silver Layer

```python
# Celda 11: Leer warnings de Silver layer

df_silver_read = spark.read.format("delta").load(silver_path)

print(f"⚠️ Total warnings detectados: {df_silver_read.count()}")

# Contar warnings por sensor
warnings_by_sensor = (df_silver_read
  .groupBy("sensor_id")
  .count()
  .orderBy(col("count").desc())
)

display(warnings_by_sensor)
```

### Paso 6.3: Agregaciones en Tiempo Real

```python
# Celda 12: Calcular promedios por sensor

from pyspark.sql.functions import avg, max, min, count

sensor_stats = (df_bronze_read
  .groupBy("sensor_id")
  .agg(
    count("*").alias("total_events"),
    avg("temperature").alias("avg_temperature"),
    max("temperature").alias("max_temperature"),
    min("temperature").alias("min_temperature"),
    avg("humidity").alias("avg_humidity"),
    avg("latency_seconds").alias("avg_latency_sec")
  )
  .orderBy("sensor_id")
)

display(sensor_stats)
```

---

## Parte 7: Agregaciones con Ventanas de Tiempo (10 min)

### Paso 7.1: Calcular Métricas por Ventana de 1 Minuto

```python
# Celda 13: Agregaciones con window

from pyspark.sql.functions import window, count, avg

# Stream con agregaciones por ventanas de 1 minuto
df_windowed = (df_transformed
  .withWatermark("event_timestamp", "2 minutes")  # Esperar hasta 2 min por eventos tardíos
  .groupBy(
    window("event_timestamp", "1 minute"),  # Ventana de 1 minuto
    "sensor_id"
  )
  .agg(
    count("*").alias("event_count"),
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
  )
)

# Escribir a tabla agregada
aggregated_path = "/tmp/lab02_gold_aggregated"
checkpoint_aggregated = "/tmp/lab02_checkpoints_aggregated"

query_aggregated = (df_windowed
  .writeStream
  .format("delta")
  .outputMode("update")  # update para agregaciones
  .option("checkpointLocation", checkpoint_aggregated)
  .start(aggregated_path)
)

print(f"✓ Stream de agregaciones iniciado: {query_aggregated.id}")
```

### Paso 7.2: Consultar Agregaciones

```python
# Celda 14: Ver agregaciones por ventana

df_aggregated_read = spark.read.format("delta").load(aggregated_path)

# Últimas ventanas de tiempo
display(
  df_aggregated_read
    .select(
      "window.start",
      "window.end",
      "sensor_id",
      "event_count",
      "avg_temperature",
      "avg_humidity"
    )
    .orderBy(col("window.start").desc())
    .limit(20)
)
```

---

## Parte 8: Monitoreo de Streams (5 min)

### Paso 8.1: Ver Progreso Detallado

```python
# Celda 15: Monitorear progreso del stream

import json

# Ver progreso del stream Bronze
progress = query_bronze.lastProgress

if progress:
    print("📊 Último progreso del stream Bronze:")
    print(f"  Batch ID: {progress['batchId']}")
    print(f"  Timestamp: {progress['timestamp']}")
    print(f"  Eventos procesados: {progress['numInputRows']}")
    print(f"  Tasa de procesamiento: {progress['processedRowsPerSecond']:.2f} rows/sec")
    print(f"  Latencia batch: {progress['batchDuration']} ms")
    
    # Ver detalles de sources
    for source in progress['sources']:
        print(f"\n  Source: {source['description']}")
        print(f"    Start offset: {source.get('startOffset', 'N/A')}")
        print(f"    End offset: {source.get('endOffset', 'N/A')}")
        print(f"    Num input rows: {source.get('numInputRows', 0)}")
```

### Paso 8.2: Crear Dashboard de Monitoreo

```python
# Celda 16: Dashboard de métricas

import time

def show_stream_dashboard():
    """Muestra dashboard con métricas de todos los streams"""
    
    print("=" * 80)
    print("📊 DASHBOARD DE STREAMING")
    print("=" * 80)
    
    for stream in spark.streams.active:
        name = stream.name if stream.name else stream.id
        print(f"\n🔷 Stream: {name}")
        print(f"   ID: {stream.id}")
        print(f"   Status: {stream.status['message']}")
        
        if stream.lastProgress:
            progress = stream.lastProgress
            print(f"   Batch ID: {progress['batchId']}")
            print(f"   Eventos procesados: {progress.get('numInputRows', 0)}")
            if 'processedRowsPerSecond' in progress:
                print(f"   Throughput: {progress['processedRowsPerSecond']:.2f} rows/sec")
    
    # Estadísticas de datos
    print(f"\n📂 DATOS GUARDADOS")
    print(f"   Bronze (total): {spark.read.format('delta').load(bronze_path).count()} eventos")
    print(f"   Silver (warnings): {spark.read.format('delta').load(silver_path).count()} eventos")
    print(f"   Gold (agregado): {spark.read.format('delta').load(aggregated_path).count()} ventanas")
    
    print("=" * 80)

# Mostrar dashboard
show_stream_dashboard()
```

---

## Parte 9: Detener Streams y Limpieza (3 min)

### Paso 9.1: Detener Streams

```python
# Celda 17: Detener todos los streams

for stream in spark.streams.active:
    print(f"Deteniendo stream: {stream.id}")
    stream.stop()
    
print("✓ Todos los streams detenidos")
```

### Paso 9.2: Verificar Datos Finales

```python
# Celda 18: Estadísticas finales

print("📊 RESUMEN FINAL")
print("=" * 60)

# Bronze
bronze_count = spark.read.format("delta").load(bronze_path).count()
print(f"Total eventos procesados: {bronze_count}")

# Silver
silver_count = spark.read.format("delta").load(silver_path).count()
warning_percentage = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
print(f"Total warnings: {silver_count} ({warning_percentage:.1f}%)")

# Gold
gold_count = spark.read.format("delta").load(aggregated_path).count()
print(f"Ventanas agregadas: {gold_count}")

print("=" * 60)
```

### Paso 9.3: Limpiar Datos (Opcional)

```python
# Celda 19: Limpiar datos de prueba

# ⚠️ Descomenta solo si quieres eliminar todos los datos del lab
# dbutils.fs.rm(bronze_path, recurse=True)
# dbutils.fs.rm(silver_path, recurse=True)
# dbutils.fs.rm(aggregated_path, recurse=True)
# dbutils.fs.rm(checkpoint_bronze, recurse=True)
# dbutils.fs.rm(checkpoint_silver, recurse=True)
# dbutils.fs.rm(checkpoint_aggregated, recurse=True)
# print("✓ Datos eliminados")
```

---

## 📝 Resumen del Laboratorio

### ✅ Lo que Aprendiste

1. **Integración Event Hub + Databricks**:
   - Configurar conexión con Event Hubs
   - Leer stream en tiempo real
   - Parsear eventos JSON

2. **Structured Streaming**:
   - Transformaciones en streaming
   - Filtrado de eventos
   - Agregaciones con ventanas de tiempo

3. **Arquitectura Medallion**:
   - **Bronze**: Datos raw de Event Hub
   - **Silver**: Datos filtrados (solo warnings)
   - **Gold**: Datos agregados por ventana temporal

4. **Operaciones**:
   - Checkpointing automático
   - Monitoreo de streams
   - Manejo de watermarks para eventos tardíos

### 🎯 Arquitectura Implementada

```
Event Hub (Productor continuo)
    │
    ↓
Databricks Structured Streaming
    │
    ├──→ Bronze Layer (Delta): Todos los eventos
    │
    ├──→ Silver Layer (Delta): Solo warnings
    │
    └──→ Gold Layer (Delta): Agregaciones por minuto
```

### 💡 Conceptos Clave

- **Watermark**: Permite manejar eventos que llegan tarde (hasta 2 minutos en nuestro caso)
- **OutputMode**:
  - `append`: Para nuevos eventos (Bronze, Silver)
  - `update`: Para agregaciones que se actualizan (Gold)
- **Checkpoint**: Permite reanudar stream exactamente donde se quedó tras fallo

### 🚀 Mejoras Posibles

1. **Alertas en Tiempo Real**: Enviar warnings a Azure Functions o Logic Apps
2. **Dashboard en Power BI**: Conectar a Delta Lake para visualización
3. **ML en Streaming**: Detectar anomalías con modelos ML
4. **Retención automática**: Eliminar eventos antiguos con `VACUUM`

### 📚 Próximos Pasos

- **Explorar**: Databricks Auto Loader para archivos
- **Integrar**: Stream Analytics para procesamiento sin código
- **Optimizar**: Z-ORDER en Delta Lake para queries más rápidas

---

**Tiempo estimado**: 45-50 minutos  
**Nivel de dificultad**: ⭐⭐⭐☆☆ (Intermedio)
