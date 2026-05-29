# Demo 1: Envío de Eventos a Event Hub

**Duración**: 25 minutos  
**Objetivo**: Demostrar cómo un productor envía eventos a Event Hub en tiempo real

---

## Preparación (5 min)

### Requisitos Previos

- Event Hub namespace creado
- Event Hub "demo-telemetry" con 4 particiones
- Connection string con permiso "Send"

### Configuración Inicial

```bash
# Variables
NAMESPACE="my-eventhub-namespace"
EVENTHUB_NAME="demo-telemetry"
RESOURCE_GROUP="rg-eventhubs-demo"

# Crear namespace (si no existe)
az eventhubs namespace create \
  --name $NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --location eastus \
  --sku Standard \
  --capacity 2

# Crear Event Hub
az eventhubs eventhub create \
  --name $EVENTHUB_NAME \
  --namespace-name $NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --partition-count 4 \
  --message-retention 1

# Obtener connection string
az eventhubs namespace authorization-rule keys list \
  --namespace-name $NAMESPACE \
  --name RootManageSharedAccessKey \
  --resource-group $RESOURCE_GROUP \
  --query primaryConnectionString -o tsv
```

---

## Parte 1: Productor Simple (10 min)

### Código del Productor

Crear `producer_simple.py`:

```python
"""
Productor simple que envía eventos simulados de telemetría IoT
"""
import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# Configuración
CONNECTION_STRING = "<tu-connection-string>"
EVENTHUB_NAME = "demo-telemetry"

async def generate_telemetry_event(device_id):
    """
    Genera un evento simulado de telemetría de un dispositivo IoT
    
    Args:
        device_id: ID del dispositivo
        
    Returns:
        dict: Evento con temperatura, humedad y timestamp
    """
    return {
        "device_id": device_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),  # °C
        "humidity": round(random.uniform(30.0, 80.0), 2),     # %
        "pressure": round(random.uniform(980.0, 1020.0), 2)   # hPa
    }

async def send_events_simple():
    """
    Envía eventos uno por uno (no optimizado, solo para demo)
    """
    # Crear cliente productor
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    
    async with producer:
        # Simular 10 dispositivos enviando datos
        for device_id in range(1, 11):
            # Generar evento
            event_data = await generate_telemetry_event(f"device_{device_id:03d}")
            
            # Crear evento con partition key (todos los eventos del mismo dispositivo van a la misma partición)
            event = EventData(json.dumps(event_data))
            event.partition_key = f"device_{device_id:03d}"
            
            # Enviar
            await producer.send_event(event)
            
            print(f"✓ Enviado: Device {device_id:03d} - Temp: {event_data['temperature']}°C")
            
            # Pausa para visualizar eventos individuales
            await asyncio.sleep(0.5)
    
    print(f"\n✅ Se enviaron 10 eventos exitosamente")

if __name__ == "__main__":
    asyncio.run(send_events_simple())
```

### Ejecutar Demo

```bash
# Instalar dependencias
pip install azure-eventhub

# Ejecutar productor
python producer_simple.py
```

**Salida Esperada**:
```
✓ Enviado: Device 001 - Temp: 23.45°C
✓ Enviado: Device 002 - Temp: 28.12°C
✓ Enviado: Device 003 - Temp: 19.87°C
...
✅ Se enviaron 10 eventos exitosamente
```

---

## Parte 2: Productor con Batching (10 min)

### Código Optimizado

Crear `producer_batched.py`:

```python
"""
Productor optimizado que usa batching para alto throughput
"""
import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

CONNECTION_STRING = "<tu-connection-string>"
EVENTHUB_NAME = "demo-telemetry"

async def generate_telemetry_event(device_id):
    """Genera evento simulado"""
    return {
        "device_id": device_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2),
        "pressure": round(random.uniform(980.0, 1020.0), 2)
    }

async def send_events_batched(num_devices=100, events_per_device=100):
    """
    Envía eventos en batches para máxima eficiencia
    
    Args:
        num_devices: Número de dispositivos simulados
        events_per_device: Eventos que cada dispositivo envía
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    
    total_events = 0
    start_time = datetime.utcnow()
    
    async with producer:
        # Crear batch
        batch = await producer.create_batch()
        
        for device_id in range(1, num_devices + 1):
            for _ in range(events_per_device):
                # Generar evento
                event_data = await generate_telemetry_event(f"device_{device_id:03d}")
                event = EventData(json.dumps(event_data))
                event.partition_key = f"device_{device_id:03d}"
                
                try:
                    # Intentar agregar al batch actual
                    batch.add(event)
                    total_events += 1
                except ValueError:
                    # Batch lleno, enviar y crear nuevo
                    await producer.send_batch(batch)
                    print(f"📦 Batch enviado ({batch.size_in_bytes} bytes)")
                    
                    batch = await producer.create_batch()
                    batch.add(event)
                    total_events += 1
        
        # Enviar batch final
        if len(batch) > 0:
            await producer.send_batch(batch)
            print(f"📦 Batch final enviado ({batch.size_in_bytes} bytes)")
    
    # Calcular throughput
    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    throughput = total_events / duration
    
    print(f"\n✅ Resumen:")
    print(f"   Total eventos: {total_events:,}")
    print(f"   Duración: {duration:.2f} segundos")
    print(f"   Throughput: {throughput:.2f} eventos/seg")

if __name__ == "__main__":
    # Enviar 10,000 eventos (100 dispositivos × 100 eventos c/u)
    asyncio.run(send_events_batched(num_devices=100, events_per_device=100))
```

### Ejecutar y Comparar

```bash
python producer_batched.py
```

**Salida Esperada**:
```
📦 Batch enviado (245632 bytes)
📦 Batch enviado (248901 bytes)
...
📦 Batch final enviado (128456 bytes)

✅ Resumen:
   Total eventos: 10,000
   Duración: 8.34 segundos
   Throughput: 1199.04 eventos/seg
```

---

## Verificación en Azure Portal

1. Navegar a: Event Hub → "demo-telemetry" → "Process Data" → "Metrics"
2. Ver gráfica de "Incoming Messages" (debe mostrar pico reciente)
3. Ver "Incoming Bytes" para confirmar throughput

**Métricas Esperadas**:
- Incoming Messages: ~10,000 (en los últimos 5 minutos)
- Incoming Bytes: ~1-2 MB
- Throttled Requests: 0 (si hay >0, necesitas más TUs)

---

## Preguntas para el Equipo

1. **¿Cuántos eventos por segundo esperan en producción?**
   - Ayuda a dimensionar Throughput Units

2. **¿Qué partition key usarían?**
   - Por usuario, dispositivo, sesión, etc.

3. **¿Cómo manejarían eventos que fallan al enviarse?**
   - Dead letter queue, logs, retry

---

## Próximo Demo

En [Demo 2](demo-02-consumo-visualizacion.md) veremos cómo **consumir** estos eventos y visualizar métricas en tiempo real.
