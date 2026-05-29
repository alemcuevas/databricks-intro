# Lab 01: Productor y Consumer Básicos de Event Hub

**Duración estimada**: 30-40 minutos  
**Nivel**: Principiante  
**Objetivo**: Crear un productor y consumer de Event Hub desde cero, entendiendo el flujo completo de eventos

---

## 📋 Prerequisitos

- Python 3.11 o superior instalado
- Suscripción de Azure con permisos de Contributor
- Azure CLI instalado y autenticado (`az login`)
- Editor de código (VS Code recomendado)

---

## Parte 1: Crear Infraestructura en Azure (10 min)

### Paso 1.1: Crear Resource Group

```bash
# Variables
RESOURCE_GROUP="rg-eventhubs-lab01"
LOCATION="eastus"
NAMESPACE="eh-lab01-$RANDOM"
EVENTHUB_NAME="telemetry"
STORAGE_ACCOUNT="checkpoints$RANDOM"

# Crear resource group
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION
```

### Paso 1.2: Crear Event Hub Namespace

```bash
# Crear namespace (Standard tier)
az eventhubs namespace create \
  --name $NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard \
  --capacity 1
```

**Explicación**:
- `--sku Standard`: Tier estándar (hasta 20 throughput units)
- `--capacity 1`: 1 throughput unit inicial (1 MB/s ingress, 2 MB/s egress)

### Paso 1.3: Crear Event Hub

```bash
# Crear Event Hub con 4 particiones
az eventhubs eventhub create \
  --name $EVENTHUB_NAME \
  --namespace-name $NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --partition-count 4 \
  --message-retention 1
```

**Explicación**:
- `--partition-count 4`: 4 particiones para paralelismo
- `--message-retention 1`: Retención de 1 día

### Paso 1.4: Obtener Connection String

```bash
# Obtener connection string con permisos de lectura y escritura
CONNECTION_STRING=$(az eventhubs namespace authorization-rule keys list \
  --namespace-name $NAMESPACE \
  --name RootManageSharedAccessKey \
  --resource-group $RESOURCE_GROUP \
  --query primaryConnectionString -o tsv)

echo "Connection String guardado en variable CONNECTION_STRING"
echo $CONNECTION_STRING
```

**⚠️ IMPORTANTE**: Guarda este connection string, lo usarás en los siguientes pasos.

### Paso 1.5: Crear Storage Account para Checkpoints

```bash
# Crear storage account
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS

# Crear container para checkpoints
az storage container create \
  --name checkpoints \
  --account-name $STORAGE_ACCOUNT

# Obtener connection string del storage
STORAGE_CONNECTION_STRING=$(az storage account show-connection-string \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query connectionString -o tsv)

echo "Storage Connection String guardado"
echo $STORAGE_CONNECTION_STRING
```

---

## Parte 2: Crear Productor (10 min)

### Paso 2.1: Instalar Dependencias

Crear archivo `requirements.txt`:

```
azure-eventhub==5.11.4
```

Instalar:

```bash
pip install -r requirements.txt
```

### Paso 2.2: Crear Productor Simple

Crear archivo `producer.py`:

```python
"""
Productor de eventos para Event Hub
Genera eventos simulados de sensores IoT
"""
import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# CONFIGURACIÓN: Reemplaza con tu connection string
CONNECTION_STRING = "<tu-connection-string-aqui>"
EVENTHUB_NAME = "telemetry"

async def generate_sensor_event(sensor_id):
    """
    Genera un evento simulado de un sensor IoT
    
    Args:
        sensor_id: ID del sensor (ej: "sensor_001")
    
    Returns:
        dict: Evento con temperatura, humedad, presión
    """
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(18.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 70.0), 2),
        "pressure": round(random.uniform(1000.0, 1020.0), 2),
        "status": random.choice(["ok", "ok", "ok", "warning"])  # 75% ok, 25% warning
    }

async def send_events(num_sensors=10, events_per_sensor=5):
    """
    Envía eventos de múltiples sensores a Event Hub
    
    Args:
        num_sensors: Número de sensores simulados
        events_per_sensor: Eventos que cada sensor genera
    """
    # Crear cliente productor
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    
    total_events = 0
    
    async with producer:
        print(f"🚀 Iniciando envío de eventos...")
        print(f"   Sensores: {num_sensors}")
        print(f"   Eventos por sensor: {events_per_sensor}")
        print(f"   Total eventos: {num_sensors * events_per_sensor}\n")
        
        # Crear batch
        batch = await producer.create_batch()
        
        for sensor_num in range(1, num_sensors + 1):
            sensor_id = f"sensor_{sensor_num:03d}"
            
            for _ in range(events_per_sensor):
                # Generar evento
                event_data = await generate_sensor_event(sensor_id)
                
                # Crear evento con partition key
                # Todos los eventos del mismo sensor van a la misma partición
                event = EventData(json.dumps(event_data))
                event.partition_key = sensor_id
                
                try:
                    # Agregar al batch
                    batch.add(event)
                    total_events += 1
                    
                except ValueError:
                    # Batch lleno, enviar y crear nuevo
                    await producer.send_batch(batch)
                    print(f"📦 Batch enviado ({total_events} eventos hasta ahora)")
                    
                    batch = await producer.create_batch()
                    batch.add(event)
                    total_events += 1
        
        # Enviar batch final
        if len(batch) > 0:
            await producer.send_batch(batch)
            print(f"📦 Batch final enviado\n")
    
    print(f"✅ Completado: {total_events} eventos enviados exitosamente")

if __name__ == "__main__":
    # Ejecutar productor
    asyncio.run(send_events(num_sensors=10, events_per_sensor=5))
```

### Paso 2.3: Ejecutar Productor

```bash
# Asegúrate de haber reemplazado CONNECTION_STRING en producer.py
python producer.py
```

**Salida esperada**:
```
🚀 Iniciando envío de eventos...
   Sensores: 10
   Eventos por sensor: 5
   Total eventos: 50

📦 Batch enviado (50 eventos hasta ahora)

✅ Completado: 50 eventos enviados exitosamente
```

### Paso 2.4: Verificar en Azure Portal

1. Navegar a: Azure Portal → Event Hub → "telemetry" → Metrics
2. Agregar métrica: **"Incoming Messages"**
3. Deberías ver ~50 mensajes en los últimos minutos

**❓ Pregunta de reflexión**: ¿Por qué usamos `partition_key = sensor_id`? ¿Qué pasaría sin partition key?

---

## Parte 3: Crear Consumer (15 min)

### Paso 3.1: Instalar Dependencias Adicionales

Actualizar `requirements.txt`:

```
azure-eventhub==5.11.4
azure-eventhub-checkpointstoreblob-aio==1.1.4
```

Instalar:

```bash
pip install -r requirements.txt
```

### Paso 3.2: Crear Consumer

Crear archivo `consumer.py`:

```python
"""
Consumer de eventos de Event Hub
Lee eventos y muestra estadísticas básicas
"""
import asyncio
import json
from collections import defaultdict
from datetime import datetime
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

# CONFIGURACIÓN: Reemplaza con tus connection strings
EVENT_HUB_CONNECTION_STRING = "<tu-eventhub-connection-string>"
STORAGE_CONNECTION_STRING = "<tu-storage-connection-string>"
EVENTHUB_NAME = "telemetry"
CONSUMER_GROUP = "$Default"

# Estadísticas globales
stats = {
    "total_events": 0,
    "events_by_sensor": defaultdict(int),
    "events_by_partition": defaultdict(int),
    "warnings": 0,
    "start_time": None
}

async def on_event(partition_context, event):
    """
    Callback que procesa cada evento recibido
    
    Args:
        partition_context: Contexto de la partición
        event: Evento recibido de Event Hub
    """
    global stats
    
    # Inicializar start_time en primer evento
    if stats["start_time"] is None:
        stats["start_time"] = datetime.utcnow()
    
    # Parsear evento
    event_data = json.loads(event.body_as_str())
    
    # Actualizar estadísticas
    stats["total_events"] += 1
    stats["events_by_sensor"][event_data["sensor_id"]] += 1
    stats["events_by_partition"][partition_context.partition_id] += 1
    
    if event_data.get("status") == "warning":
        stats["warnings"] += 1
    
    # Imprimir evento individual
    print(f"📥 [{partition_context.partition_id}] {event_data['sensor_id']}: "
          f"Temp={event_data['temperature']}°C, "
          f"Status={event_data['status']}")
    
    # Checkpoint cada 10 eventos
    if stats["total_events"] % 10 == 0:
        await partition_context.update_checkpoint(event)
        print(f"✓ Checkpoint guardado (total: {stats['total_events']} eventos)\n")
        
        # Mostrar estadísticas cada 10 eventos
        await print_statistics()

async def print_statistics():
    """Imprime estadísticas actuales"""
    elapsed = (datetime.utcnow() - stats["start_time"]).total_seconds()
    throughput = stats["total_events"] / elapsed if elapsed > 0 else 0
    
    print("=" * 60)
    print("📊 ESTADÍSTICAS")
    print("=" * 60)
    print(f"Total eventos: {stats['total_events']}")
    print(f"Warnings: {stats['warnings']} ({(stats['warnings']/stats['total_events']*100):.1f}%)")
    print(f"Throughput: {throughput:.2f} eventos/seg")
    print(f"\n📦 Por partición:")
    for partition, count in sorted(stats["events_by_partition"].items()):
        print(f"   Partition {partition}: {count}")
    print(f"\n🔧 Por sensor (top 5):")
    for sensor, count in sorted(stats["events_by_sensor"].items(), 
                                  key=lambda x: x[1], reverse=True)[:5]:
        print(f"   {sensor}: {count}")
    print("=" * 60 + "\n")

async def on_error(partition_context, error):
    """Maneja errores del consumer"""
    print(f"❌ Error en partition {partition_context.partition_id}: {error}")

async def consume_events():
    """Inicia el consumer y procesa eventos continuamente"""
    
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
    
    print("🎯 Consumer iniciado")
    print("   Esperando eventos...")
    print("   Presiona Ctrl+C para detener\n")
    
    async with client:
        await client.receive(
            on_event=on_event,
            on_error=on_error,
            starting_position="-1"  # -1 = empezar desde los más recientes
        )

if __name__ == "__main__":
    try:
        asyncio.run(consume_events())
    except KeyboardInterrupt:
        print("\n\n✅ Consumer detenido")
        if stats["total_events"] > 0:
            asyncio.run(print_statistics())
```

### Paso 3.3: Ejecutar Consumer

**Terminal 1** (Consumer):
```bash
python consumer.py
```

**Terminal 2** (Productor - genera más eventos):
```bash
python producer.py
```

**Salida esperada del Consumer**:
```
🎯 Consumer iniciado
   Esperando eventos...
   Presiona Ctrl+C para detener

📥 [2] sensor_001: Temp=24.5°C, Status=ok
📥 [0] sensor_002: Temp=22.3°C, Status=ok
📥 [3] sensor_003: Temp=26.8°C, Status=warning
...
✓ Checkpoint guardado (total: 10 eventos)

============================================================
📊 ESTADÍSTICAS
============================================================
Total eventos: 10
Warnings: 2 (20.0%)
Throughput: 15.23 eventos/seg

📦 Por partición:
   Partition 0: 3
   Partition 1: 2
   Partition 2: 3
   Partition 3: 2

🔧 Por sensor (top 5):
   sensor_001: 1
   sensor_002: 1
   sensor_003: 1
   sensor_004: 1
   sensor_005: 1
============================================================
```

---

## Parte 4: Experimentos (5 min)

### Experimento 1: Enviar Más Eventos

Modifica `producer.py`:
```python
asyncio.run(send_events(num_sensors=20, events_per_sensor=10))  # 200 eventos
```

Ejecuta y observa:
- ¿Cuánto tarda en procesar 200 eventos?
- ¿El throughput aumenta?

### Experimento 2: Sin Partition Key

Modifica `producer.py`, comenta la línea:
```python
# event.partition_key = sensor_id  # <-- Comentar esta línea
```

Ejecuta productor y consumer:
- ¿Los eventos del mismo sensor van a la misma partición?
- ¿Qué impacto tiene esto?

### Experimento 3: Múltiples Consumers

Abre **3 terminales** y ejecuta `consumer.py` en cada una.

Observa:
- ¿Cómo se distribuyen las particiones?
- ¿Cada consumer procesa eventos diferentes?
- ¿Qué pasa si detienes uno?

---

## Parte 5: Limpieza (2 min)

```bash
# Eliminar resource group (elimina todos los recursos)
az group delete --name $RESOURCE_GROUP --yes --no-wait
```

---

## 📝 Resumen del Laboratorio

### ✅ Lo que Aprendiste

1. **Crear infraestructura**:
   - Event Hub namespace y Event Hub
   - Storage para checkpoints

2. **Productor**:
   - Enviar eventos con batching
   - Usar partition keys para ordenamiento
   - Manejar batches llenos

3. **Consumer**:
   - Leer eventos con checkpointing
   - Procesar eventos en paralelo (múltiples particiones)
   - Mantener estadísticas

4. **Conceptos clave**:
   - Partition key garantiza orden por entidad
   - Checkpoints permiten recuperación tras fallos
   - Múltiples consumers se distribuyen particiones automáticamente

### 🎯 Preguntas de Reflexión

1. ¿Qué pasa si el productor envía eventos más rápido que el consumer procesa?
2. ¿Cuántos consumers puedes tener antes de que algunos queden idle?
3. ¿Qué sucede si el checkpoint falla? ¿Se pierden eventos?

### 📚 Próximos Pasos

- **[Lab 02](lab-02-databricks-streaming.md)**: Consumir eventos con Databricks Structured Streaming
- **[Demo 01](../demos/demo-01-envio-eventos.md)**: Ver productor optimizado
- **[Demo 02](../demos/demo-02-consumo-visualizacion.md)**: Consumer con métricas avanzadas

---

**Tiempo estimado**: 30-40 minutos  
**Nivel de dificultad**: ⭐⭐☆☆☆ (Principiante)
