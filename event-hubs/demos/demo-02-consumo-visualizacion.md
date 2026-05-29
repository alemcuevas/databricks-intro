# Demo 2: Consumo de Eventos y Visualización

**Duración**: 25 minutos  
**Objetivo**: Demostrar cómo consumir eventos de Event Hub y visualizar métricas en tiempo real

---

## Preparación (5 min)

### Requisitos Previos

- Demo 1 completado (Event Hub con eventos)
- Azure Blob Storage para checkpoints
- Connection string con permiso "Listen"

### Configuración Storage

```bash
# Crear storage account para checkpoints
STORAGE_ACCOUNT="checkpointsstorage$RANDOM"
CONTAINER="checkpoints"

az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location eastus \
  --sku Standard_LRS

az storage container create \
  --name $CONTAINER \
  --account-name $STORAGE_ACCOUNT

# Obtener connection string
az storage account show-connection-string \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query connectionString -o tsv
```

---

## Parte 1: Consumer Básico (10 min)

### Código del Consumer

Crear `consumer_simple.py`:

```python
"""
Consumer básico que lee eventos de Event Hub y los imprime
"""
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

# Configuración
EVENT_HUB_CONNECTION_STRING = "<tu-connection-string>"
STORAGE_CONNECTION_STRING = "<storage-connection-string>"
EVENTHUB_NAME = "demo-telemetry"
CONSUMER_GROUP = "$Default"

async def on_event(partition_context, event):
    """
    Callback que se ejecuta por cada evento recibido
    
    Args:
        partition_context: Contexto de la partición
        event: Evento recibido
    """
    import json
    
    # Parsear evento
    event_data = json.loads(event.body_as_str())
    
    # Imprimir información
    print(f"📥 Partition {partition_context.partition_id}:")
    print(f"   Device: {event_data['device_id']}")
    print(f"   Temp: {event_data['temperature']}°C")
    print(f"   Humidity: {event_data['humidity']}%")
    print(f"   Timestamp: {event_data['timestamp']}")
    print(f"   Offset: {event.offset}")
    print()
    
    # Guardar checkpoint cada 10 eventos
    if int(event.sequence_number) % 10 == 0:
        await partition_context.update_checkpoint(event)
        print(f"✓ Checkpoint guardado en partition {partition_context.partition_id}")

async def on_partition_initialize(partition_context):
    """Se ejecuta cuando consumer empieza a leer una partición"""
    print(f"🚀 Iniciando lectura de partition {partition_context.partition_id}")

async def on_partition_close(partition_context, reason):
    """Se ejecuta cuando consumer deja de leer una partición"""
    print(f"🛑 Cerrando partition {partition_context.partition_id}. Razón: {reason}")

async def on_error(partition_context, error):
    """Se ejecuta cuando ocurre un error"""
    print(f"❌ Error en partition {partition_context.partition_id}: {error}")

async def consume_events():
    """Inicia el consumer y lee eventos continuamente"""
    
    # Crear checkpoint store
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        conn_str=STORAGE_CONNECTION_STRING,
        container_name="checkpoints"
    )
    
    # Crear consumer client
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )
    
    print("🎯 Consumer iniciado. Esperando eventos...")
    print("   Presiona Ctrl+C para detener\n")
    
    async with client:
        # Recibir eventos
        await client.receive(
            on_event=on_event,
            on_partition_initialize=on_partition_initialize,
            on_partition_close=on_partition_close,
            on_error=on_error,
            starting_position="-1"  # -1 = empezar desde los más recientes
        )

if __name__ == "__main__":
    try:
        asyncio.run(consume_events())
    except KeyboardInterrupt:
        print("\n✅ Consumer detenido")
```

### Ejecutar Consumer

```bash
# Instalar dependencias
pip install azure-eventhub azure-eventhub-checkpointstoreblob-aio

# Ejecutar consumer
python consumer_simple.py
```

**Salida Esperada**:
```
🎯 Consumer iniciado. Esperando eventos...
   Presiona Ctrl+C para detener

🚀 Iniciando lectura de partition 0
🚀 Iniciando lectura de partition 1
🚀 Iniciando lectura de partition 2
🚀 Iniciando lectura de partition 3

📥 Partition 2:
   Device: device_045
   Temp: 24.67°C
   Humidity: 55.32%
   Timestamp: 2024-05-29T10:15:30.123Z
   Offset: 1234567

📥 Partition 1:
   Device: device_023
   Temp: 21.89°C
   ...
```

---

## Parte 2: Agregación en Tiempo Real (10 min)

### Consumer con Métricas

Crear `consumer_with_metrics.py`:

```python
"""
Consumer que agrega métricas en ventanas de tiempo
"""
import asyncio
import json
from datetime import datetime
from collections import defaultdict
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

# Configuración
EVENT_HUB_CONNECTION_STRING = "<tu-connection-string>"
STORAGE_CONNECTION_STRING = "<storage-connection-string>"
EVENTHUB_NAME = "demo-telemetry"
CONSUMER_GROUP = "$Default"

# Métricas globales
metrics = {
    "total_events": 0,
    "events_by_partition": defaultdict(int),
    "temperature_sum": 0.0,
    "temperature_count": 0,
    "start_time": datetime.utcnow()
}

async def on_event(partition_context, event):
    """
    Procesa evento y actualiza métricas
    """
    global metrics
    
    # Parsear evento
    event_data = json.loads(event.body_as_str())
    
    # Actualizar métricas
    metrics["total_events"] += 1
    metrics["events_by_partition"][partition_context.partition_id] += 1
    metrics["temperature_sum"] += event_data["temperature"]
    metrics["temperature_count"] += 1
    
    # Checkpoint cada 50 eventos
    if metrics["total_events"] % 50 == 0:
        await partition_context.update_checkpoint(event)
        await print_metrics()

async def print_metrics():
    """Imprime métricas actuales"""
    elapsed = (datetime.utcnow() - metrics["start_time"]).total_seconds()
    throughput = metrics["total_events"] / elapsed if elapsed > 0 else 0
    avg_temp = metrics["temperature_sum"] / metrics["temperature_count"] if metrics["temperature_count"] > 0 else 0
    
    print("\n" + "="*60)
    print("📊 MÉTRICAS EN TIEMPO REAL")
    print("="*60)
    print(f"⏱️  Tiempo transcurrido: {elapsed:.1f} segundos")
    print(f"📨 Total eventos procesados: {metrics['total_events']:,}")
    print(f"⚡ Throughput: {throughput:.2f} eventos/seg")
    print(f"🌡️  Temperatura promedio: {avg_temp:.2f}°C")
    print(f"\n📦 Eventos por partición:")
    for partition, count in sorted(metrics["events_by_partition"].items()):
        percentage = (count / metrics["total_events"]) * 100
        bar = "█" * int(percentage / 2)
        print(f"   Partition {partition}: {count:>6,} ({percentage:>5.1f}%) {bar}")
    print("="*60 + "\n")

async def consume_with_metrics():
    """Inicia consumer con agregación de métricas"""
    
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        conn_str=STORAGE_CONNECTION_STRING,
        container_name="checkpoints"
    )
    
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )
    
    print("🎯 Consumer con métricas iniciado")
    print("   Métricas se actualizan cada 50 eventos\n")
    
    # Resetear start time
    metrics["start_time"] = datetime.utcnow()
    
    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1"
        )

if __name__ == "__main__":
    try:
        asyncio.run(consume_with_metrics())
    except KeyboardInterrupt:
        print("\n\n✅ Consumer detenido")
        asyncio.run(print_metrics())  # Imprimir métricas finales
```

### Ejecutar Productor + Consumer

Terminal 1 (Productor):
```bash
python producer_batched.py
```

Terminal 2 (Consumer con métricas):
```bash
python consumer_with_metrics.py
```

**Salida Esperada del Consumer**:
```
============================================================
📊 MÉTRICAS EN TIEMPO REAL
============================================================
⏱️  Tiempo transcurrido: 12.3 segundos
📨 Total eventos procesados: 2,500
⚡ Throughput: 203.25 eventos/seg
🌡️  Temperatura promedio: 24.87°C

📦 Eventos por partición:
   Partition 0:    625 ( 25.0%) ████████████
   Partition 1:    634 ( 25.4%) ████████████
   Partition 2:    618 ( 24.7%) ████████████
   Partition 3:    623 ( 24.9%) ████████████
============================================================
```

---

## Verificación en Azure Portal

### Ver Checkpoints

1. Navegar a: Storage Account → "checkpoints" container
2. Ver archivos generados:
   - `demo-telemetry/$Default/0` (checkpoint partition 0)
   - `demo-telemetry/$Default/1` (checkpoint partition 1)
   - etc.

### Ver Métricas de Consumer

1. Navegar a: Event Hub → "demo-telemetry" → Metrics
2. Agregar métrica: "Outgoing Messages"
3. Comparar con "Incoming Messages"
   - Deben ser similares (consumer está al día)

---

## Discusión con el Equipo (5 min)

### Preguntas Clave

1. **¿Qué harían con estos eventos en producción?**
   - Ejemplos: Guardar en base de datos, enviar alertas, agregaciones

2. **¿Qué pasa si el consumer se cae?**
   - Respuesta: Reinicia desde último checkpoint (no pierde eventos)

3. **¿Cómo escalarían si llegan 100,000 eventos/seg?**
   - Respuesta: Más particiones + múltiples instancias del consumer

4. **¿Necesitan procesamiento idempotente?**
   - Respuesta: Sí si operaciones no son idempotentes naturalmente (usar deduplicación)

---

## Próximos Pasos

- Integrar con Databricks para análisis histórico
- Configurar Stream Analytics para alertas
- Implementar KEDA en AKS para auto-scaling

**Recursos**:
- [Documentación de Event Hubs](../docs/)
- [Labs prácticos](../labs/)
- [Checklist para KT](../../resources/checklist-kt-eventhubs.md)
