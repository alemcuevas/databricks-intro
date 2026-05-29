# Arquitectura e Integración con Event Hubs

## Índice

1. [Patrones de Arquitectura](#patrones-de-arquitectura)
2. [Integración con Azure Databricks](#integración-con-azure-databricks)
3. [Integración con Azure Kubernetes Service (AKS)](#integración-con-azure-kubernetes-service-aks)
4. [Arquitecturas de Referencia](#arquitecturas-de-referencia)

---

## Patrones de Arquitectura

### Event-Driven Architecture (EDA)

```
┌─────────────┐
│   Eventos   │ Orden creada, Pago completado, Email enviado
└──────┬──────┘
       │
   ┌───▼───────────────┐
   │   EVENT HUB       │ ← Hub central de eventos
   └───┬───────────┬───┘
       │           │
  ┌────▼────┐  ┌──▼──────────┐
  │ Service │  │ Service B   │
  │ A       │  │ (consumer)  │
  └─────────┘  └─────────────┘
```

**Ventajas**:
- Desacoplamiento de servicios
- Escalabilidad independiente
- Resiliencia ante fallos

### Lambda Architecture

```
                    EVENT HUB
                        │
         ┌──────────────┼──────────────┐
         │              │              │
    ┌────▼─────┐   ┌────▼────┐   ┌────▼─────┐
    │  Speed   │   │  Batch  │   │ Serving  │
    │  Layer   │   │  Layer  │   │  Layer   │
    │ (Stream  │   │(Databri │   │ (Synapse │
    │Analytics)│   │  cks)   │   │   SQL)   │
    └────┬─────┘   └────┬────┘   └────┬─────┘
         │              │              │
         └──────────────┼──────────────┘
                        │
                  ┌─────▼──────┐
                  │ Power BI   │
                  └────────────┘
```

**Speed Layer**: Procesa eventos en tiempo real (<1 segundo)
**Batch Layer**: Procesa lotes grandes con exactitud (5-60 minutos)
**Serving Layer**: Combina vistas para consultas

### Kappa Architecture (Simplificada)

```
EVENT HUB → Stream Processing (Databricks) → Serving Layer
              ↓
        Reprocessing desde histórico
```

**Ventaja sobre Lambda**: Un solo pipeline (menos complejidad)

---

## Integración con Azure Databricks

### Opción 1: Structured Streaming con Event Hubs

```python
# Configuración de conexión
connectionString = "Endpoint=sb://my-namespace.servicebus.windows.net/;..."
ehConf = {
  'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.consumerGroup': 'databricks-consumer'
}

# Leer stream
df = (spark
  .readStream
  .format("eventhubs")
  .options(**ehConf)
  .load()
)

# Schema del evento
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

schema = StructType([
  StructField("userId", IntegerType()),
  StructField("action", StringType()),
  StructField("timestamp", LongType())
])

# Parsear body de Event Hub
parsed_df = (df
  .select(from_json(col("body").cast("string"), schema).alias("data"))
  .select("data.*")
)

# Procesar stream
query = (parsed_df
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/tmp/checkpoints/events")
  .start("/mnt/data/processed_events")
)
```

### Checkpoint Management

```python
# Databricks maneja checkpoints automáticamente
checkpoint_location = "/mnt/checkpoints/event-hub-stream"

# Si necesitas reiniciar desde tiempo específico
ehConf['eventhubs.startingPosition'] = json.dumps({
  "offset": "-1",  # Latest
  # "offset": "@1622505600000",  # Timestamp específico
  # "offset": "12345",  # Offset específico
  # "enqueuedTime": "2024-05-29T10:00:00Z"
})
```

### Ventana de Tiempo para Agregaciones

```python
from pyspark.sql.functions import window, count

# Contar eventos por ventana de 5 minutos
windowed_counts = (parsed_df
  .withColumn("timestamp_parsed", col("timestamp").cast("timestamp"))
  .groupBy(window("timestamp_parsed", "5 minutes"), "action")
  .count()
)

query = (windowed_counts
  .writeStream
  .outputMode("update")  # Solo cambios incrementales
  .format("delta")
  .option("checkpointLocation", "/tmp/checkpoints/windowed")
  .start("/mnt/data/windowed_events")
)
```

### Watermarking (Manejo de Datos Tardíos)

```python
# Esperar hasta 10 minutos por eventos retrasados
windowed_with_watermark = (parsed_df
  .withWatermark("timestamp_parsed", "10 minutes")
  .groupBy(window("timestamp_parsed", "5 minutes"), "action")
  .count()
)
```

### Escritura de Vuelta a Event Hub

```python
# Escribir resultados a otro Event Hub
output_ehConf = {
  'eventhubs.connectionString': encrypt(output_connection_string)
}

query = (windowed_counts
  .selectExpr("to_json(struct(*)) AS body")
  .writeStream
  .format("eventhubs")
  .options(**output_ehConf)
  .option("checkpointLocation", "/tmp/checkpoints/output")
  .start()
)
```

### Monitoreo en Databricks

```python
# Ver estado del stream
query.status

# Métricas detalladas
display(query.lastProgress)

# Stop stream
query.stop()

# Reiniciar con recovery automático
query.awaitTermination()
```

---

## Integración con Azure Kubernetes Service (AKS)

### Escenario: Microservicio Consumidor en AKS

**Arquitectura**:
```
Event Hub → [AKS Cluster]
             ├── Pod 1 (consumer, partitions 0-2)
             ├── Pod 2 (consumer, partitions 3-5)
             └── Pod 3 (consumer, partitions 6-7)
```

### Deployment con Python Consumer

**1. Dockerfile**
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py"]
```

**2. requirements.txt**
```
azure-eventhub==5.11.4
azure-eventhub-checkpointstoreblob-aio==1.1.4
azure-identity==1.14.0
```

**3. consumer.py**
```python
import asyncio
import os
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.identity.aio import DefaultAzureCredential

async def on_event(partition_context, event):
    print(f"Partition {partition_context.partition_id}: {event.body_as_str()}")
    
    # Procesar evento
    await process_event(event)
    
    # Checkpoint cada 10 eventos
    if int(event.sequence_number) % 10 == 0:
        await partition_context.update_checkpoint(event)

async def main():
    credential = DefaultAzureCredential()
    
    checkpoint_store = BlobCheckpointStore(
        blob_account_url=os.environ["BLOB_STORAGE_URL"],
        container_name="checkpoints",
        credential=credential
    )
    
    client = EventHubConsumerClient(
        fully_qualified_namespace=os.environ["EVENT_HUB_NAMESPACE"],
        eventhub_name=os.environ["EVENT_HUB_NAME"],
        consumer_group="$Default",
        checkpoint_store=checkpoint_store,
        credential=credential
    )
    
    async with client:
        await client.receive(on_event=on_event, starting_position="-1")

if __name__ == "__main__":
    asyncio.run(main())
```

**4. Kubernetes Deployment**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventhub-consumer
spec:
  replicas: 3  # 3 pods para distribuir particiones
  selector:
    matchLabels:
      app: eventhub-consumer
  template:
    metadata:
      labels:
        app: eventhub-consumer
        azure.workload.identity/use: "true"  # Managed Identity
    spec:
      serviceAccountName: eventhub-consumer-sa
      containers:
      - name: consumer
        image: myregistry.azurecr.io/eventhub-consumer:v1
        env:
        - name: EVENT_HUB_NAMESPACE
          value: "my-namespace.servicebus.windows.net"
        - name: EVENT_HUB_NAME
          value: "telemetry"
        - name: BLOB_STORAGE_URL
          value: "https://mystorageaccount.blob.core.windows.net"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

**5. Workload Identity Setup**
```bash
# Crear managed identity
az identity create --name eventhub-consumer-id --resource-group myRG

# Asignar roles
IDENTITY_CLIENT_ID=$(az identity show --name eventhub-consumer-id --resource-group myRG --query clientId -o tsv)

az role assignment create \
  --role "Azure Event Hubs Data Receiver" \
  --assignee $IDENTITY_CLIENT_ID \
  --scope /subscriptions/.../resourceGroups/myRG/providers/Microsoft.EventHub/namespaces/my-namespace

# Configurar Kubernetes ServiceAccount
kubectl create serviceaccount eventhub-consumer-sa

kubectl annotate serviceaccount eventhub-consumer-sa \
  azure.workload.identity/client-id=$IDENTITY_CLIENT_ID
```

### Escalado Automático con KEDA

**KEDA ScaledObject** escala pods según lag de Event Hub:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: eventhub-consumer-scaler
spec:
  scaleTargetRef:
    name: eventhub-consumer
  minReplicaCount: 1
  maxReplicaCount: 10  # Max 10 pods
  triggers:
  - type: azure-eventhub
    metadata:
      namespace: my-namespace
      eventHubName: telemetry
      consumerGroup: $Default
      unprocessedEventThreshold: '1000'  # Escala si >1000 eventos pendientes
      activationUnprocessedEventThreshold: '10'  # Activa scaling con >10 eventos
      connectionFromEnv: EVENT_HUB_CONNECTION_STRING
```

**Comportamiento**:
```
Eventos pendientes: 0       → 1 pod
Eventos pendientes: 500     → 1 pod
Eventos pendientes: 2000    → 2 pods (>1000 threshold)
Eventos pendientes: 10000   → 10 pods
Eventos pendientes: 100     → Scale down a 1 pod
```

---

## Arquitecturas de Referencia

### Arquitectura 1: IoT Telemetry Pipeline

```
┌─────────────────────┐
│  10,000 IoT Devices │
│  (envían métricas   │
│   cada 10 seg)      │
└──────────┬──────────┘
           │
       ┌───▼────────────────┐
       │   EVENT HUB        │
       │ 32 partitions      │
       │ 10 throughput units│
       └───┬───────────┬────┘
           │           │
  ┌────────▼─────┐  ┌─▼──────────────┐
  │ Stream       │  │ Databricks     │
  │ Analytics    │  │ (batch every   │
  │ (alerts <1s) │  │  5 min)        │
  └────┬─────────┘  └─┬──────────────┘
       │              │
  ┌────▼─────────┐  ┌▼───────────────┐
  │ Azure        │  │ Delta Lake     │
  │ Functions    │  │ (análisis      │
  │ (send alert) │  │  histórico)    │
  └──────────────┘  └─┬──────────────┘
                      │
                 ┌────▼─────┐
                 │ Power BI │
                 └──────────┘
```

**Configuración Recomendada**:
- Partitions: 32 (una por cada 300-400 dispositivos)
- Throughput Units: 10 (100,000 eventos/min)
- Consumer Groups: 
  - `stream-analytics` (alertas)
  - `databricks-batch` (análisis)
  - `archival` (captura a Storage)

### Arquitectura 2: E-commerce con Event-Driven Microservices

```
┌────────────┐
│  Frontend  │
└──────┬─────┘
       │
  ┌────▼──────────┐
  │ Order Service │
  └────┬──────────┘
       │ Emite: OrderCreated, OrderCancelled
       │
   ┌───▼───────────────┐
   │   EVENT HUB       │
   └───┬───────────┬───┘
       │           │
┌──────▼─────┐ ┌──▼─────────────┐
│ Inventory  │ │ Notification   │
│ Service    │ │ Service        │
│ (AKS)      │ │ (AKS)          │
└──────┬─────┘ └──┬─────────────┘
       │          │
  Actualiza    Envía email
  stock        al usuario
```

**Patrón**: Event Sourcing + CQRS

**Ventajas**:
- Servicios desacoplados
- Retry automático si servicio cae
- Audit trail completo (replay de eventos)

### Arquitectura 3: Real-Time Analytics con Lambda

```
                 EVENT HUB
                     │
      ┌──────────────┼──────────────┐
      │              │              │
┌─────▼──────┐  ┌────▼────┐   ┌────▼─────┐
│  Stream    │  │Databricks│  │ Captura  │
│  Analytics │  │ (batch  │  │ a ADLS   │
│  (hot path)│  │  cold   │  │ (Avro)   │
│            │  │  path)  │  │          │
└─────┬──────┘  └────┬────┘  └──────────┘
      │              │
      │         ┌────▼─────────┐
      │         │  Delta Lake  │
      │         │  (histórico) │
      │         └────┬─────────┘
      │              │
  ┌───▼──────────────▼───┐
  │   Synapse Analytics  │
  │   (vista combinada)  │
  └───┬──────────────────┘
      │
  ┌───▼────┐
  │Power BI│
  └────────┘
```

**Casos de Uso**:
- Dashboards en tiempo real (<5 seg) + análisis histórico preciso
- Comparación de tendencias: último minuto vs. último mes
- Detección de anomalías con contexto histórico

---

## Preguntas Clave para el KT

Cuando recibas el Knowledge Transfer del partner sobre la arquitectura, pregunta:

### Sobre Event Hub

1. **¿Cuántos Event Hubs hay en el namespace?**
   - Nombres de cada Event Hub
   - Propósito de cada uno

2. **¿Cómo se particionan los eventos?**
   - Partition key definido
   - Número de particiones

3. **¿Quiénes son los productores?**
   - Aplicaciones que envían eventos
   - Volumen y frecuencia

### Sobre Databricks

4. **¿Qué cluster procesa los eventos?**
   - Cluster interactivo vs. Jobs cluster
   - Configuración (workers, VM size)

5. **¿Dónde se almacenan los checkpoints?**
   - Path en DBFS o ADLS
   - Estrategia de backup

6. **¿Cuál es la latencia objetivo?**
   - Tiempo desde evento a resultado
   - SLA definido

### Sobre AKS (si aplica)

7. **¿Cómo se escala el consumer?**
   - KEDA configurado
   - Límites de scaling

8. **¿Cómo se autentica el consumer?**
   - Managed Identity (recomendado)
   - Connection string (menos seguro)

9. **¿Hay retry logic implementado?**
   - Manejo de errores transitorios
   - Dead letter queue

---

## Próximos Pasos

- **[04-produccion-eventos.md](04-produccion-eventos.md)**: Patrones de productores
- **[05-consumo-eventos.md](05-consumo-eventos.md)**: Patrones de consumidores
- **[06-monitoreo-troubleshooting.md](06-monitoreo-troubleshooting.md)**: Operaciones y troubleshooting

**Tiempo estimado de lectura**: 45-60 minutos
