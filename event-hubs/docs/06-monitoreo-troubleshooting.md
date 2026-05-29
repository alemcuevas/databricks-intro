# Monitoreo y Troubleshooting

## Métricas Clave

### Métricas de Throughput

| Métrica | Descripción | Valor Normal | Alerta Si... |
|---------|-------------|--------------|--------------|
| `Incoming Messages` | Eventos recibidos/min | Según carga | Caída súbita >50% |
| `Outgoing Messages` | Eventos leídos/min | ~= Incoming | Mucho menor (lag) |
| `Throttled Requests` | Requests rechazados | 0 | >0 (necesitas más TUs) |
| `Server Errors` | Errores 5xx | <0.1% | >1% |
| `User Errors` | Errores 4xx (auth, etc.) | <0.5% | >2% |

### Consultas KQL en Log Analytics

```kusto
// Ver eventos por hora
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.EVENTHUB"
| summarize Count=count() by bin(TimeGenerated, 1h), OperationName
| render timechart

// Detectar throttling
AzureDiagnostics
| where Category == "OperationalLogs"
| where OperationName == "ServerBusyException"
| summarize ThrottledRequests=count() by bin(TimeGenerated, 5m)
| render timechart

// Latencia de procesamiento
AzureDiagnostics
| where Category == "RuntimeAuditLogs"
| extend LatencyMs = todouble(Duration)
| summarize avg(LatencyMs), percentile(LatencyMs, 95) by bin(TimeGenerated, 5m)
| render timechart
```

---

## Problemas Comunes y Soluciones

### Problema 1: Throttling (ServerBusyException)

**Síntomas**:
```python
azure.eventhub.exceptions.EventHubError: 
    The server is busy. Error code: 50002
```

**Causas**:
- Throughput Units insuficientes
- Picos de tráfico inesperados
- Partition key con hotspot

**Solución**:
```bash
# 1. Verificar métricas actuales
az monitor metrics list \
  --resource /subscriptions/.../Microsoft.EventHub/namespaces/my-namespace \
  --metric IncomingMessages,OutgoingMessages,ThrottledRequests

# 2. Aumentar TUs
az eventhubs namespace update \
  --name my-namespace \
  --capacity 20  # Aumentar de 10 a 20 TUs

# 3. Habilitar auto-inflate
az eventhubs namespace update \
  --name my-namespace \
  --enable-auto-inflate true \
  --maximum-throughput-units 40
```

### Problema 2: Consumer Lag (Eventos Acumulados)

**Síntomas**:
- `Outgoing Messages` << `Incoming Messages`
- Retraso en procesamiento (>5 minutos)

**Diagnóstico**:
```python
# Ver lag por partición
from azure.eventhub import EventHubConsumerClient

client = EventHubConsumerClient.from_connection_string(conn_str, consumer_group="$Default", eventhub_name="telemetry")

partition_ids = client.get_partition_ids()
for partition_id in partition_ids:
    properties = client.get_partition_properties(partition_id)
    print(f"Partition {partition_id}:")
    print(f"  Last enqueued sequence: {properties.last_enqueued_sequence_number}")
    print(f"  Last enqueued offset: {properties.last_enqueued_offset}")
```

**Solución**:
1. **Escalar consumidores** (agregar más instancias)
2. **Optimizar procesamiento** (reducir tiempo por evento)
3. **Aumentar paralelismo** (procesar eventos en paralelo)

### Problema 3: Checkpoint Failures

**Síntomas**:
```
BlobStorageError: The specified container does not exist
```

**Solución**:
```bash
# Verificar que existe el container
az storage container show \
  --name checkpoints \
  --account-name mystorageaccount

# Crear si no existe
az storage container create \
  --name checkpoints \
  --account-name mystorageaccount

# Verificar permisos
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <consumer-identity> \
  --scope /subscriptions/.../storageAccounts/mystorageaccount
```

### Problema 4: Eventos Perdidos

**Síntomas**:
- Eventos enviados por productor no aparecen en consumer
- Gaps en secuencia de offsets

**Diagnóstico**:
```kusto
// Ver eventos por partition
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.EVENTHUB"
| where Category == "ArchiveLogs" or Category == "OperationalLogs"
| summarize Count=count() by PartitionId, bin(TimeGenerated, 5m)
| render barchart
```

**Causas Comunes**:
1. **Retention expirado** → Eventos eliminados después de 1-90 días
2. **Consumer leyendo desde wrong offset**
3. **Producer recibió throttling** y no reintentó

**Solución**:
```python
# Verificar offset del consumer
async def check_consumer_position():
    from azure.storage.blob import BlobServiceClient
    
    blob_service = BlobServiceClient(account_url=storage_url, credential=credential)
    container = blob_service.get_container_client("checkpoints")
    
    blobs = container.list_blobs(name_starts_with="my-eventhub/my-consumer-group/")
    
    for blob in blobs:
        print(f"{blob.name}: {blob.metadata}")
```

---

## Alertas Recomendadas

### Azure Monitor Alerts

```bash
# Alerta por Throttling
az monitor metrics alert create \
  --name "EventHub-Throttling" \
  --resource-group myRG \
  --scopes /subscriptions/.../Microsoft.EventHub/namespaces/my-namespace \
  --condition "count ThrottledRequests > 10" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --action email admin@example.com

# Alerta por Consumer Lag
az monitor metrics alert create \
  --name "EventHub-ConsumerLag" \
  --resource-group myRG \
  --scopes /subscriptions/.../Microsoft.EventHub/namespaces/my-namespace \
  --condition "count IncomingMessages > count OutgoingMessages + 1000" \
  --window-size 10m \
  --evaluation-frequency 5m
```

---

## Checklist de Operaciones

### Diario
- ✅ Verificar métricas de throughput (incoming/outgoing)
- ✅ Revisar alertas de throttling
- ✅ Monitorear consumer lag

### Semanal
- ✅ Analizar patrones de tráfico
- ✅ Revisar costos de TUs
- ✅ Validar que checkpoints se guardan correctamente
- ✅ Verificar que no hay consumer groups inactivos

### Mensual
- ✅ Revisar estrategia de particionamiento
- ✅ Evaluar necesidad de auto-inflate
- ✅ Auditar permisos (RBAC, Managed Identities)
- ✅ Revisar retention period (¿necesitas más/menos días?)

---

## Herramientas de Diagnóstico

### 1. Event Hubs Explorer (Azure Portal)

Navigate to: Event Hub → "Process Data" → "Query Data"

### 2. CLI para Debug

```bash
# Ver propiedades del namespace
az eventhubs namespace show --name my-namespace --resource-group myRG

# Ver particiones
az eventhubs eventhub show --name telemetry --namespace-name my-namespace --resource-group myRG

# Ver consumer groups
az eventhubs eventhub consumer-group list --eventhub-name telemetry --namespace-name my-namespace --resource-group myRG
```

### 3. Application Insights

```python
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

configure_azure_monitor(connection_string=conn_str)
tracer = trace.get_tracer(__name__)

async def monitored_consumer(partition_context, event):
    with tracer.start_as_current_span("process_event") as span:
        span.set_attribute("partition_id", partition_context.partition_id)
        span.set_attribute("offset", event.offset)
        
        try:
            await process(event)
            span.set_attribute("status", "success")
        except Exception as e:
            span.set_attribute("status", "failed")
            span.set_attribute("error", str(e))
            raise
```

---

## Preguntas para el Partner en el KT

1. **¿Qué métricas monitorean actualmente?**
   - ¿Alertas configuradas?
   - ¿Umbrales definidos?

2. **¿Cuál es el SLA de latencia?**
   - Tiempo desde evento producido hasta consumido

3. **¿Cómo manejan incidentes?**
   - Runbook de escalamiento
   - Procedimiento de failover

4. **¿Hay disaster recovery configurado?**
   - Geo-replication
   - Namespace secundario

5. **¿Cómo se hace troubleshooting de eventos perdidos?**
   - Logs habilitados
   - Auditoría de permisos

---

## Tiempo estimado de lectura: 25 minutos
