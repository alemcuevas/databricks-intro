# Producción de Eventos

## Mejores Prácticas para Productores

### 1. Batching Eficiente

```python
from azure.eventhub import EventHubProducerClient, EventData

async def send_events_batched(events):
    producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name="telemetry")
    
    async with producer:
        batch = await producer.create_batch()
        
        for event in events:
            try:
                batch.add(EventData(event))
            except ValueError:  # Batch lleno
                await producer.send_batch(batch)
                batch = await producer.create_batch()
                batch.add(EventData(event))
        
        # Enviar batch final
        if len(batch) > 0:
            await producer.send_batch(batch)
```

### 2. Partition Key Strategy

```python
# Por usuario (mantiene orden de eventos del usuario)
event = EventData(json.dumps(data))
event.partition_key = f"user_{user_id}"

# Por sesión (agrupa interacciones de una sesión)
event.partition_key = f"session_{session_id}"

# Por dispositivo IoT
event.partition_key = f"device_{device_id}"
```

### 3. Retry con Backoff

```python
from azure.core.exceptions import ServiceBusyError
import asyncio
import random

async def send_with_retry(producer, batch, max_retries=5):
    for attempt in range(max_retries):
        try:
            await producer.send_batch(batch)
            return True
        except ServiceBusyError:
            if attempt == max_retries - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            await asyncio.sleep(wait)
    return False
```

### 4. Schema Validation

```python
from jsonschema import validate, ValidationError

event_schema = {
    "type": "object",
    "properties": {
        "userId": {"type": "integer"},
        "action": {"type": "string", "enum": ["click", "view", "purchase"]},
        "timestamp": {"type": "integer"}
    },
    "required": ["userId", "action", "timestamp"]
}

def send_validated_event(data):
    try:
        validate(instance=data, schema=event_schema)
        event = EventData(json.dumps(data))
        await producer.send_event(event)
    except ValidationError as e:
        logger.error(f"Invalid event: {e}")
```

---

## Patrones de Producción

### Patrón 1: Fire-and-Forget (Alta Throughput)

```python
# No espera confirmación, máxima velocidad
async def fire_and_forget(events):
    tasks = []
    for batch in create_batches(events):
        task = asyncio.create_task(producer.send_batch(batch))
        tasks.append(task)
    
    # Ejecutar todos en paralelo
    await asyncio.gather(*tasks, return_exceptions=True)
```

**Cuándo usar**: Telemetría no crítica, logs

### Patrón 2: Guaranteed Delivery

```python
async def send_with_confirmation(events):
    results = []
    for event in events:
        try:
            await producer.send_event(EventData(event))
            results.append({"status": "sent", "event": event})
        except Exception as e:
            results.append({"status": "failed", "event": event, "error": str(e)})
    
    return results
```

**Cuándo usar**: Transacciones financieras, órdenes críticas

### Patrón 3: Transactional Outbox

```python
# Garantiza consistencia entre DB y Event Hub
def process_order(order):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Guardar en DB
        cursor.execute("INSERT INTO orders VALUES (%s, %s)", (order.id, order.data))
        
        # 2. Guardar evento en outbox table
        cursor.execute("""
            INSERT INTO outbox_events (event_type, payload, status)
            VALUES ('OrderCreated', %s, 'pending')
        """, (json.dumps(order),))
        
        conn.commit()
    except Exception:
        conn.rollback()
        raise

# Worker procesa outbox
async def outbox_worker():
    while True:
        events = fetch_pending_events_from_outbox()
        
        for event in events:
            try:
                await producer.send_event(EventData(event.payload))
                mark_event_as_sent(event.id)
            except Exception:
                retry_later(event.id)
        
        await asyncio.sleep(1)
```

---

## Monitoreo de Productores

```python
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

configure_azure_monitor(connection_string=app_insights_conn_str)
tracer = trace.get_tracer(__name__)

async def send_with_telemetry(events):
    with tracer.start_as_current_span("send_events") as span:
        span.set_attribute("event_count", len(events))
        
        try:
            await producer.send_batch(create_batch(events))
            span.set_attribute("status", "success")
        except Exception as e:
            span.set_attribute("status", "failed")
            span.set_attribute("error", str(e))
            raise
```

---

## Tiempo estimado de lectura: 20 minutos
