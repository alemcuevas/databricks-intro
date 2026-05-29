# Conceptos Clave de Event Hubs

## Índice

1. [Particiones](#particiones)
2. [Throughput Units](#throughput-units)
3. [Consumer Groups](#consumer-groups)
4. [Productores y Eventos](#productores-y-eventos)
5. [Consumidores y Checkpointing](#consumidores-y-checkpointing)
6. [Esquemas de Datos](#esquemas-de-datos)

---

## Particiones

### ¿Qué es una Partición?

Una partición es una **secuencia ordenada de eventos** dentro de un Event Hub. Es la unidad fundamental de paralelismo y escalabilidad.

```
┌──────────────────────────────────────────────┐
│           EVENT HUB: "telemetry"             │
├──────────────────────────────────────────────┤
│ Partition 0: [evt1] → [evt2] → [evt3] → ... │
│ Partition 1: [evt4] → [evt5] → [evt6] → ... │
│ Partition 2: [evt7] → [evt8] → [evt9] → ... │
│ Partition 3: [evt10]→ [evt11]→ [evt12]→ ... │
└──────────────────────────────────────────────┘
```

**Analogía**: Imagina un supermercado con 4 cajas (particiones). Los clientes (eventos) se distribuyen entre las cajas para procesar más rápido. Cada caja procesa clientes en orden de llegada, pero no hay orden global entre cajas.

### Propiedades de las Particiones

**1. Ordenamiento Garantizado**
- Eventos en la **misma partición** mantienen orden de llegada
- Eventos en **distintas particiones** no tienen orden garantizado

**Ejemplo Correcto**:
```
Usuario A → Partition 0: [Login] → [AddToCart] → [Checkout]  ✓
Usuario B → Partition 1: [Login] → [AddToCart] → [Checkout]  ✓
```

**Ejemplo Incorrecto (sin partition key)**:
```
Usuario A → Partition 0: [Login]
Usuario A → Partition 2: [AddToCart]   ⚠️ Puede llegar antes que Login
Usuario A → Partition 1: [Checkout]    ⚠️ Orden perdido
```

**2. Offset Secuencial**
- Cada evento en una partición recibe un offset único incremental
- Offsets: 0, 1, 2, 3, ... (nunca se resetean)
- Consumidores pueden leer desde un offset específico

**3. Inmutabilidad**
- Una vez escrito, un evento no puede modificarse
- Solo puede ser leído o eliminado (después de retention period)

### Cuántas Particiones Necesito

**Regla General**:
```
Número de Particiones = Throughput Deseado (MB/s) / Throughput por Partición
```

**Throughput por Partición**:
- **Ingress (escritura)**: 1 MB/s o 1000 eventos/s (lo que sea menor)
- **Egress (lectura)**: 2 MB/s

**Ejemplos de Dimensionamiento**:

| Escenario | Eventos/seg | Tamaño Evento | Particiones Mínimas | Recomendado |
|-----------|-------------|---------------|---------------------|-------------|
| IoT Pequeño | 5,000 | 500 bytes | 3 | 4 |
| Logs de App | 20,000 | 1 KB | 20 | 24 |
| Clickstream | 100,000 | 2 KB | 200 | 256 |
| Telemetría Masiva | 1,000,000 | 500 bytes | 500 | 512 |

**⚠️ Consideraciones Importantes**:

**No Puedes Reducir Particiones**
- Solo puedes aumentar (no disminuir)
- Planifica con crecimiento en mente
- Migración a nuevo Event Hub si necesitas reducir

**Más Particiones = Más Costo de Consumo**
- Cada consumer procesa múltiples particiones
- Con 100 partitions, cada consumer puede manejar 10-20 particiones
- Aumenta complejidad de balanceo

**Impacto en Latencia**
- 1-10 partitions: latencia <10ms
- 100-500 partitions: latencia 10-50ms
- 1000+ partitions: latencia >50ms (coordinación overhead)

### Estrategia de Partition Key

**Sin Partition Key** (distribución automática):
```python
# Distribuye eventos round-robin entre particiones
await producer.send_batch(events)  # Sin partition_key
```

✅ **Ventajas**: Máximo throughput, balance perfecto
❌ **Desventajas**: Sin ordenamiento entre eventos relacionados

**Con Partition Key** (agrupación lógica):
```python
# Todos los eventos del mismo usuario van a la misma partición
event_data = EventData("user_clicked")
event_data.partition_key = f"user_{user_id}"
await producer.send_event(event_data)
```

✅ **Ventajas**: Ordenamiento garantizado por clave
❌ **Desventajas**: Posible hotspot si una clave tiene mucho tráfico

**Partition Key: Mejores Prácticas**

| Caso de Uso | Partition Key | Razón |
|-------------|---------------|--------|
| Eventos de usuario | `user_id` | Mantener sesión ordenada |
| IoT Telemetry | `device_id` | Secuencia temporal por dispositivo |
| Logs de aplicación | `instance_id` | Correlación de logs |
| Transacciones | `transaction_id` | ACID a nivel transacción |
| Clickstream | `session_id` | Journey del usuario ordenado |

**Evitar Hotspots**:

```python
# ❌ MAL: Todos los eventos de una empresa van a misma partición
partition_key = company_id  # Si company_1 tiene 90% del tráfico → bottleneck

# ✓ BIEN: Combinar con sub-entidad
partition_key = f"{company_id}_{device_id}"  # Mejor distribución
```

---

## Throughput Units

### Qué es un Throughput Unit (TU)

Un **Throughput Unit** es la unidad de capacidad de Event Hubs. Define cuántos datos pueden fluir por segundo.

**1 Throughput Unit proporciona**:
- **Ingress (entrada)**: 1 MB/s o 1000 eventos/s
- **Egress (salida)**: 2 MB/s o 4096 eventos/s

```
┌─────────────────────────────┐
│   1 Throughput Unit (TU)    │
├─────────────────────────────┤
│ Ingress:  1 MB/s IN         │
│          1000 eventos/s     │
├─────────────────────────────┤
│ Egress:   2 MB/s OUT        │
│          4096 eventos/s     │
└─────────────────────────────┘
```

### Cálculo de TUs Necesarios

**Fórmula**:
```
TUs Requeridos = max(
    Ingress MB/s / 1 MB/s,
    Ingress eventos/s / 1000,
    Egress MB/s / 2 MB/s
)
```

**Ejemplo 1: Blog con Comentarios**
- 5,000 eventos/seg de entrada
- Tamaño promedio: 2 KB
- 2 consumidores leyendo

```
Ingress = 5,000 * 2 KB = 10 MB/s → 10 TUs por ingress
Ingress = 5,000 eventos/s → 5 TUs por eventos
Egress = 10 MB/s * 2 consumidores = 20 MB/s → 10 TUs por egress

TUs Necesarios = max(10, 5, 10) = 10 TUs
```

**Ejemplo 2: Telemetría IoT**
- 500,000 eventos/seg de entrada
- Tamaño promedio: 500 bytes
- 5 consumidores (Databricks, Stream Analytics, Functions, Archive, Analytics)

```
Ingress = 500,000 * 500 bytes = 250 MB/s → 250 TUs por ingress
Ingress = 500,000 eventos/s → 500 TUs por eventos (límite)
Egress = 250 MB/s * 5 consumidores = 1250 MB/s → 625 TUs por egress

TUs Necesarios = max(250, 500, 625) = 625 TUs
```

### Tiers de Event Hubs

| Característica | Standard | Premium | Dedicated |
|----------------|----------|---------|-----------|
| **Max TUs** | 40 (hasta 100 con soporte) | 16 PUs* | Ilimitado |
| **Particiones** | 32 por Event Hub | 100 por Event Hub | 1024 por Event Hub |
| **Retención** | 7 días | 90 días | 90 días |
| **Consumer Groups** | 20 | 100 | Ilimitado |
| **Private Link** | ❌ | ✅ | ✅ |
| **CMK (Customer Managed Keys)** | ❌ | ✅ | ✅ |
| **Precio Base** | ~$22/TU/mes | ~$1375/PU/mes | Contactar |
| **Use Case** | Desarrollo, cargas pequeñas | Producción, compliance | Extrema escala |

*PU = Processing Unit (equivalente a 4 TUs aproximadamente)

### Auto-Inflate

**Auto-Inflate** ajusta automáticamente los TUs según la demanda.

**Configuración**:
```bash
az eventhubs namespace update \
  --name my-namespace \
  --enable-auto-inflate true \
  --maximum-throughput-units 20
```

**Ejemplo de Comportamiento**:

```
Hora    Carga      TUs Configurados    TUs Efectivos    Costo
────────────────────────────────────────────────────────────
08:00   1 MB/s     Min: 2, Max: 10     2 TUs            $0.06
10:00   5 MB/s     Auto-inflate        5 TUs            $0.15
12:00   8 MB/s     Auto-inflate        8 TUs            $0.24
14:00   3 MB/s     Auto-deflate        3 TUs            $0.09
18:00   1 MB/s     Auto-deflate        2 TUs            $0.06
```

✅ **Ventajas**:
- Sin intervención manual
- Respuesta a picos de tráfico
- Reducción automática en horas valle

❌ **Limitaciones**:
- Ajuste puede tardar 1-2 minutos
- Costo puede ser impredecible
- No previene throttling en picos instantáneos

### Throttling y ServerBusy Errors

**Qué Ocurre al Exceder TUs**:

```
Configurado: 5 TUs (5 MB/s ingress)
Carga Real: 8 MB/s

Resultado:
- Primeros 5 MB/s: procesados ✓
- Siguientes 3 MB/s: rechazados con error "ServerBusyException" ❌
```

**Errores Comunes**:

```python
# Error típico al exceder TUs
azure.eventhub.exceptions.EventHubError: 
    The server is busy. Please retry later. Error code: 50002 (ServerBusy)
```

**Estrategias de Mitigación**:

**1. Retry con Backoff Exponencial**
```python
from azure.eventhub import EventHubProducerClient
from azure.core.exceptions import ServiceBusyError
import time

max_retries = 5
for attempt in range(max_retries):
    try:
        await producer.send_batch(events)
        break
    except ServiceBusyError:
        wait_time = 2 ** attempt  # 2, 4, 8, 16, 32 segundos
        time.sleep(wait_time)
```

**2. Pre-provisionar TUs Suficientes**
- Analizar tráfico histórico
- Agregar 20-30% de buffer
- Habilitar auto-inflate

**3. Rate Limiting en Productor**
```python
from time import sleep

max_events_per_second = 900  # Dejar margen (1 TU = 1000/s)
batch_size = 100

for i in range(0, len(events), batch_size):
    batch = events[i:i+batch_size]
    await producer.send_batch(batch)
    sleep(batch_size / max_events_per_second)
```

---

## Consumer Groups

### Qué es un Consumer Group

Un **Consumer Group** es una vista lógica de todo el Event Hub. Permite que múltiples aplicaciones lean los mismos eventos de forma independiente.

```
                    ┌─────────────────────┐
                    │   EVENT HUB         │
                    │ [evt1][evt2][evt3]  │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
   ┌──────────▼─────┐  ┌──────▼───────┐  ┌────▼──────────┐
   │ Consumer Group │  │ Consumer Grp │  │ Consumer Grp  │
   │  "analytics"   │  │  "archival"  │  │   "alerts"    │
   └────────┬───────┘  └──────┬───────┘  └───────┬───────┘
            │                 │                   │
   ┌────────▼────────┐ ┌─────▼──────┐   ┌────────▼───────┐
   │  Databricks     │ │ Blob Store │   │ Azure Function │
   │  (lee todo)     │ │ (archiva)  │   │ (solo errores) │
   └─────────────────┘ └────────────┘   └────────────────┘
```

### Características de Consumer Groups

**1. Aislamiento de Lectura**
- Cada consumer group mantiene su propio **offset** de lectura
- Un grupo puede estar en evento 100, otro en evento 1000
- No hay impacto entre grupos

**2. Checkpoint Independiente**
```python
# Consumer Group "analytics" guarda checkpoint
await processor.update_checkpoint(partition_id="0", offset="100")

# Consumer Group "archival" tiene su propio checkpoint
# → Puede estar en offset 50 sin problema
```

**3. Escalabilidad Horizontal**
- Múltiples instancias del mismo consumer group se distribuyen particiones
- Balanceo automático cuando instancias se agregan/quitan

**Ejemplo de Balanceo**:

```
# Escenario: 4 particiones, Consumer Group "analytics"

Solo 1 instancia:
Instance A → lee partitions [0, 1, 2, 3]

2 instancias:
Instance A → lee partitions [0, 1]
Instance B → lee partitions [2, 3]

4 instancias:
Instance A → lee partition [0]
Instance B → lee partition [1]
Instance C → lee partition [2]
Instance D → lee partition [3]

5 instancias:  ⚠️ 1 instancia queda idle
Instance A → lee partition [0]
Instance B → lee partition [1]
Instance C → lee partition [2]
Instance D → lee partition [3]
Instance E → idle (sin particiones)
```

**⚠️ Regla**: Número de instancias ≤ Número de particiones para máxima eficiencia.

### Casos de Uso de Consumer Groups

**Escenario: Pipeline de Datos de IoT**

```
                     ┌─────────────────────┐
                     │  EVENT HUB: iot     │
                     │  (sensor readings)  │
                     └──────────┬──────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
┌───────▼────────┐   ┌──────────▼──────┐   ┌──────────▼─────────┐
│ CG: realtime   │   │ CG: historical  │   │ CG: monitoring     │
└───────┬────────┘   └──────────┬──────┘   └──────────┬─────────┘
        │                       │                      │
┌───────▼────────┐   ┌──────────▼──────┐   ┌──────────▼─────────┐
│ Stream         │   │  Databricks     │   │  Azure Monitor     │
│ Analytics      │   │  (agrega en     │   │  (detecta          │
│ (alerts <1min) │   │   batches 5min) │   │   anomalías)       │
└────────────────┘   └─────────────────┘   └────────────────────┘
```

**Ventajas de Múltiples Consumer Groups**:
- **Desacoplamiento**: Cambios en "monitoring" no afectan "realtime"
- **Velocidades Distintas**: "realtime" procesa inmediato, "historical" en batches
- **Replay Independiente**: "monitoring" puede reiniciar desde evento antiguo sin afectar otros

### Límites de Consumer Groups

| Tier | Consumer Groups Máximos |
|------|-------------------------|
| Standard | 20 |
| Premium | 100 |
| Dedicated | Ilimitado |

**Cuándo Crear un Nuevo Consumer Group**:
✅ Aplicación diferente con lógica distinta
✅ Necesidad de procesar a velocidad diferente
✅ Replay independiente de eventos

❌ Misma aplicación escalando horizontalmente → Usar mismo CG

---

## Productores y Eventos

### Anatomía de un Evento

Un evento en Event Hubs consiste en:

```json
{
  "body": "<payload JSON, Avro, o bytes>",
  "properties": {
    "user_id": "12345",
    "event_type": "click",
    "source": "web"
  },
  "partition_key": "user_12345",
  "enqueued_time": "2024-05-29T10:30:00Z",
  "offset": 42,
  "sequence_number": 42
}
```

**Componentes**:

| Campo | Descripción | Tamaño Máx | Quién lo Asigna |
|-------|-------------|------------|-----------------|
| `body` | Payload del evento | 1 MB | Productor |
| `properties` | Metadata clave-valor | Incluido en 1 MB | Productor |
| `partition_key` | Clave para ruteo | 128 bytes | Productor |
| `enqueued_time` | Timestamp de escritura | - | Event Hub |
| `offset` | Posición en partición | - | Event Hub |
| `sequence_number` | Número secuencial | - | Event Hub |

### Envío de Eventos: Mejores Prácticas

**1. Batching para Eficiencia**

❌ **Ineficiente**: 1 evento por llamada
```python
for event in events:
    await producer.send_event(event)  # 1000 llamadas HTTP
```

✅ **Eficiente**: Batch de eventos
```python
from azure.eventhub import EventDataBatch

batch = await producer.create_batch()
for event in events:
    batch.add(EventData(event))
await producer.send_batch(batch)  # 1 llamada HTTP
```

**Beneficios del Batching**:
- Reduce overhead de red (~90%)
- Aumenta throughput (10x-50x)
- Reduce costo (menos operaciones)

**Tamaño Óptimo de Batch**:
- **Standard**: 256 KB por batch
- **Premium**: 1 MB por batch
- **Regla**: 100-500 eventos por batch típico

**2. Compresión de Payload**

```python
import gzip
import json

# Comprimir antes de enviar
payload = json.dumps(large_dict).encode('utf-8')
compressed = gzip.compress(payload)

event = EventData(compressed)
event.properties = {"compressed": "gzip"}
await producer.send_event(event)
```

**Ejemplo Real**:
- Evento JSON sin comprimir: 10 KB
- Evento JSON comprimido (gzip): 2 KB
- **80% reducción** → 5x más eventos por TU

**3. Retry Logic**

```python
from azure.core.exceptions import ServiceBusyError
import random

async def send_with_retry(producer, events, max_retries=5):
    for attempt in range(max_retries):
        try:
            await producer.send_batch(events)
            return True
        except ServiceBusyError:
            if attempt == max_retries - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff + jitter
            await asyncio.sleep(wait)
    return False
```

**4. Manejo de Errores**

```python
from azure.eventhub import EventDataBatch
from azure.eventhub.exceptions import EventDataSendError

try:
    batch = await producer.create_batch()
    for event in events:
        try:
            batch.add(EventData(event))
        except ValueError:  # Batch lleno
            await producer.send_batch(batch)
            batch = await producer.create_batch()
            batch.add(EventData(event))
    
    await producer.send_batch(batch)
    
except EventDataSendError as e:
    # Log eventos que fallaron
    failed_events = e.failed_events
    logger.error(f"Failed to send {len(failed_events)} events")
```

---

## Consumidores y Checkpointing

### Anatomía de un Consumer

```python
from azure.eventhub import EventHubConsumerClient

consumer = EventHubConsumerClient.from_connection_string(
    conn_str="<connection-string>",
    consumer_group="$Default",  # Consumer group name
    eventhub_name="my-eventhub"
)

def on_event(partition_context, event):
    print(f"Received event from partition {partition_context.partition_id}")
    print(f"Body: {event.body_as_str()}")
    
    # Procesar evento
    process_event(event)
    
    # Guardar checkpoint
    partition_context.update_checkpoint(event)

consumer.receive(on_event=on_event, starting_position="-1")  # -1 = latest
```

### Checkpointing: Garantía de Procesamiento

**Qué es un Checkpoint**:
Un checkpoint es una marca que indica **hasta dónde ha procesado** un consumer en una partición.

```
Partition 0: [evt1] [evt2] [evt3] [evt4] [evt5] [evt6] [evt7]
                            ▲
                    Checkpoint aquí (offset 3)
                    
Si consumer falla y reinicia:
→ Comienza desde evt4 (no reprocesa evt1-evt3)
```

**Storage de Checkpoints**:

Los checkpoints se guardan en **Azure Blob Storage**:

```
Container: checkpoints/
├── my-eventhub/
│   ├── my-consumer-group/
│   │   ├── partition-0.json  → {"offset": 1234, "sequenceNumber": 1234}
│   │   ├── partition-1.json  → {"offset": 5678, "sequenceNumber": 5678}
│   │   └── partition-2.json  → {"offset": 9012, "sequenceNumber": 9012}
```

**Frecuencia de Checkpointing**:

❌ **Muy Frecuente** (después de cada evento):
```python
def on_event(partition_context, event):
    process(event)
    partition_context.update_checkpoint(event)  # Cada evento → overhead alto
```
- **Problema**: Muchas escrituras a Blob Storage (costo, latencia)

❌ **Muy Infrecuente** (cada 10,000 eventos):
```python
event_count = 0
def on_event(partition_context, event):
    global event_count
    process(event)
    event_count += 1
    if event_count % 10000 == 0:
        partition_context.update_checkpoint(event)
```
- **Problema**: Si falla, reprocesa 10,000 eventos

✅ **Balance Óptimo** (cada 100-500 eventos o cada 5-10 segundos):
```python
import time

last_checkpoint_time = time.time()
event_count = 0

def on_event(partition_context, event):
    global last_checkpoint_time, event_count
    
    process(event)
    event_count += 1
    
    # Checkpoint cada 100 eventos O cada 10 segundos
    if event_count >= 100 or (time.time() - last_checkpoint_time) > 10:
        partition_context.update_checkpoint(event)
        last_checkpoint_time = time.time()
        event_count = 0
```

### Idempotencia: Manejo de Duplicados

Event Hubs garantiza **at-least-once delivery**:
- Evento puede procesarse 2+ veces si hay fallo antes de checkpoint

**Escenario de Duplicado**:
```
1. Consumer lee evento "transfer $100"
2. Procesa: debita $100 de cuenta
3. CRASH antes de checkpoint
4. Consumer reinicia, lee mismo evento
5. Procesa OTRA VEZ: debita $100 de nuevo ❌
```

**Soluciones**:

**1. Operaciones Idempotentes**
```python
# ❌ NO Idempotente
def process_event(event):
    balance -= event.amount  # Si se ejecuta 2 veces, resta 2 veces

# ✓ Idempotente
def process_event(event):
    if not transaction_exists(event.transaction_id):
        balance -= event.amount
        save_transaction(event.transaction_id)
```

**2. Deduplicación con Redis**
```python
import redis

redis_client = redis.Redis()

def process_event(event):
    event_id = event.properties["event_id"]
    
    # Check si ya procesamos este evento
    if redis_client.exists(f"processed:{event_id}"):
        return  # Skip duplicado
    
    # Procesar evento
    perform_business_logic(event)
    
    # Marcar como procesado (TTL = retention del Event Hub)
    redis_client.setex(f"processed:{event_id}", time=86400*7, value="1")
```

**3. Exactly-Once con Base de Datos Transaccional**
```python
def process_event(event):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar + insertar en transacción atómica
        cursor.execute("""
            INSERT INTO processed_events (event_id, processed_at)
            VALUES (%s, NOW())
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        """, (event.properties["event_id"],))
        
        if cursor.fetchone():
            # Primera vez que vemos este evento
            perform_business_logic(event)
            conn.commit()
        else:
            # Duplicado detectado
            conn.rollback()
    except Exception as e:
        conn.rollback()
        raise
```

---

## Esquemas de Datos

### Por Qué Importan los Esquemas

**Problema Sin Esquema**:
```json
// Productor A envía
{"userId": 12345, "action": "click"}

// Productor B envía
{"user_id": "12345", "eventType": "click"}

// Consumer no sabe qué formato esperar ❌
```

**Solución: Schema Registry**

Azure Schema Registry (parte de Event Hubs Premium) almacena esquemas versionados:

```
Schema Registry
├── com.example.UserEvent
│   ├── Version 1: {"userId": int, "action": string}
│   └── Version 2: {"userId": int, "action": string, "timestamp": long}
```

### Formatos de Serialización

| Formato | Tamaño | Performance | Esquema | Compatibilidad |
|---------|--------|-------------|---------|----------------|
| **JSON** | 100% | Baja | Opcional | Universal |
| **Avro** | ~40% | Media | Requerido | Hadoop, Kafka |
| **Protobuf** | ~30% | Alta | Requerido | gRPC, microservicios |
| **MessagePack** | ~60% | Alta | No | Python, Ruby, Go |

**Ejemplo Comparativo** (mismo evento):

```json
// JSON (sin compresión): 250 bytes
{
  "userId": 123456,
  "timestamp": "2024-05-29T10:30:00Z",
  "action": "page_view",
  "properties": {
    "page": "/products/shoes",
    "duration": 45,
    "device": "mobile"
  }
}

// Avro (binario): ~95 bytes (62% reducción)
// Protobuf: ~75 bytes (70% reducción)
```

### Implementación con Avro

**1. Definir Schema**
```json
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example",
  "fields": [
    {"name": "userId", "type": "long"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "action", "type": "string"},
    {"name": "properties", "type": {"type": "map", "values": "string"}}
  ]
}
```

**2. Productor con Avro**
```python
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder import AvroEncoder
from azure.eventhub import EventData

# Cliente de Schema Registry
schema_registry = SchemaRegistryClient(endpoint, credential)
encoder = AvroEncoder(schema_registry, group_name="my-group")

# Serializar con Avro
event_dict = {
    "userId": 123456,
    "timestamp": int(time.time() * 1000),
    "action": "page_view",
    "properties": {"page": "/products"}
}

encoded_data = encoder.encode(event_dict, schema=avro_schema)
event = EventData(body=encoded_data)
await producer.send_event(event)
```

**3. Consumer con Avro**
```python
decoder = AvroEncoder(schema_registry, group_name="my-group")

def on_event(partition_context, event):
    decoded_event = decoder.decode(event)
    print(decoded_event["userId"])  # 123456
```

### Evolución de Esquemas

**Compatibilidad Forward**: Consumer antiguo puede leer mensajes nuevos
```
Schema V1: {userId, action}
Schema V2: {userId, action, timestamp}  ← Agregado campo

Consumer V1 → Lee V2 → Ignora timestamp ✓
```

**Compatibilidad Backward**: Consumer nuevo puede leer mensajes antiguos
```
Schema V1: {userId, action}
Schema V2: {userId, action, timestamp: default=null}

Consumer V2 → Lee V1 → Usa timestamp=null ✓
```

**Reglas para Evolución Segura**:
✅ Agregar campos opcionales (con defaults)
✅ Eliminar campos opcionales
❌ Cambiar tipo de campo existente
❌ Renombrar campos
❌ Eliminar campos requeridos

---

## Próximos Pasos

Ahora que dominas **particiones, throughput, consumer groups** y **esquemas**, continúa con:

- **[03-arquitectura-event-hubs.md](03-arquitectura-event-hubs.md)**: Integración con Databricks, AKS, arquitecturas completas
- **[04-produccion-eventos.md](04-produccion-eventos.md)**: Patrones avanzados de productores
- **[05-consumo-eventos.md](05-consumo-eventos.md)**: Patrones avanzados de consumidores
- **[06-monitoreo-troubleshooting.md](06-monitoreo-troubleshooting.md)**: Operaciones y troubleshooting

**Tiempo estimado de lectura**: 60-75 minutos
