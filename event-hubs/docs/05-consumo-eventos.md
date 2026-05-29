# Consumo de Eventos

## Mejores Prácticas para Consumidores

### 1. Checkpoint Strategy

```python
import time

class SmartCheckpointer:
    def __init__(self, interval_seconds=10, event_count=100):
        self.interval_seconds = interval_seconds
        self.event_count = event_count
        self.last_checkpoint_time = time.time()
        self.events_since_checkpoint = 0
    
    async def maybe_checkpoint(self, partition_context, event):
        self.events_since_checkpoint += 1
        current_time = time.time()
        
        # Checkpoint si se cumple cualquier condición
        if (self.events_since_checkpoint >= self.event_count or 
            (current_time - self.last_checkpoint_time) >= self.interval_seconds):
            
            await partition_context.update_checkpoint(event)
            self.last_checkpoint_time = current_time
            self.events_since_checkpoint = 0
```

### 2. Procesamiento Paralelo

```python
import asyncio
from collections import deque

class ParallelEventProcessor:
    def __init__(self, max_workers=10):
        self.queue = deque()
        self.max_workers = max_workers
        self.checkpointer = SmartCheckpointer()
    
    async def on_event(self, partition_context, event):
        # Agregar a queue
        self.queue.append((partition_context, event))
        
        # Procesar en paralelo
        if len(self.queue) >= self.max_workers:
            await self.process_batch()
    
    async def process_batch(self):
        tasks = []
        
        while self.queue and len(tasks) < self.max_workers:
            partition_context, event = self.queue.popleft()
            task = asyncio.create_task(self.process_event(event))
            tasks.append((partition_context, event, task))
        
        # Esperar a que terminen
        for partition_context, event, task in tasks:
            await task
            await self.checkpointer.maybe_checkpoint(partition_context, event)
    
    async def process_event(self, event):
        # Tu lógica aquí
        await asyncio.sleep(0.1)  # Simula procesamiento
```

### 3. Manejo de Errores

```python
async def process_with_dlq(partition_context, event):
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            await process_event(event)
            await partition_context.update_checkpoint(event)
            return
        except TransientError:
            await asyncio.sleep(2 ** attempt)
        except PermanentError as e:
            # Enviar a Dead Letter Queue
            await send_to_dlq(event, error=str(e))
            await partition_context.update_checkpoint(event)
            return
    
    # Máximos reintentos alcanzados
    await send_to_dlq(event, error="Max retries exceeded")
    await partition_context.update_checkpoint(event)
```

---

## Patrones de Consumo

### Patrón 1: Fan-Out (Múltiples Consumidores)

```
EVENT HUB
    │
    ├──→ Consumer Group "analytics" → Databricks
    ├──→ Consumer Group "alerts" → Azure Functions
    └──→ Consumer Group "archive" → Blob Storage
```

Cada consumer group lee todos los eventos independientemente.

### Patrón 2: Competing Consumers

```
Consumer Group "processing"
    ├── Instance 1 → Partitions [0, 1]
    ├── Instance 2 → Partitions [2, 3]
    └── Instance 3 → Partitions [4, 5]
```

Múltiples instancias comparten carga dentro del mismo consumer group.

### Patrón 3: Aggregation Window

```python
from collections import defaultdict
import time

class WindowedAggregator:
    def __init__(self, window_seconds=60):
        self.window_seconds = window_seconds
        self.current_window = defaultdict(int)
        self.window_start = time.time()
    
    async def on_event(self, partition_context, event):
        current_time = time.time()
        
        # Verificar si ventana expiró
        if current_time - self.window_start >= self.window_seconds:
            await self.flush_window()
            self.current_window.clear()
            self.window_start = current_time
        
        # Agregar a ventana actual
        data = json.loads(event.body_as_str())
        self.current_window[data['action']] += 1
    
    async def flush_window(self):
        print(f"Window results: {dict(self.current_window)}")
        # Enviar a base de datos, otro Event Hub, etc.
```

---

## Deduplicación

### Opción 1: Redis Cache

```python
import redis

redis_client = redis.Redis(host='myredis.redis.cache.windows.net', port=6380, ssl=True)

async def process_deduplicated(event):
    event_id = json.loads(event.body_as_str())['event_id']
    
    # Check si ya procesado
    if redis_client.exists(f"processed:{event_id}"):
        return  # Skip duplicado
    
    # Procesar
    await process_event(event)
    
    # Marcar como procesado (TTL = 7 días)
    redis_client.setex(f"processed:{event_id}", 604800, "1")
```

### Opción 2: Database con UPSERT

```python
async def process_idempotent(event):
    data = json.loads(event.body_as_str())
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO events (event_id, data, processed_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
    """, (data['event_id'], json.dumps(data)))
    
    if cursor.fetchone():
        # Primera vez procesando este evento
        await perform_business_logic(data)
    
    conn.commit()
```

---

## Escalado de Consumidores

### Kubernetes HPA con KEDA

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: eventhub-consumer-scaler
spec:
  scaleTargetRef:
    name: eventhub-consumer
  minReplicaCount: 2
  maxReplicaCount: 16
  triggers:
  - type: azure-eventhub
    metadata:
      namespace: my-namespace
      eventHubName: telemetry
      consumerGroup: $Default
      unprocessedEventThreshold: '1000'
      connectionFromEnv: EVENT_HUB_CONNECTION_STRING
```

---

## Tiempo estimado de lectura: 20 minutos
