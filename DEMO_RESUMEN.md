# Demo Completa de Azure Event Hubs

## 📋 Resumen de Infraestructura Creada

### Event Hub Namespace
- **Nombre**: `eh-demo-202605291122`
- **Región**: East US
- **SKU**: Standard (1 Throughput Unit)
- **Kafka habilitado**: Sí
- **Endpoint**: `eh-demo-202605291122.servicebus.windows.net`

### Event Hub
- **Nombre**: `telemetry`
- **Particiones**: 4 (IDs: 0, 1, 2, 3)
- **Retención**: 7 días (168 horas)
- **Estado**: Active

### Storage Account (Checkpoints)
- **Nombre**: `checkpoint202605291122`
- **Región**: East US
- **SKU**: Standard_LRS
- **Container**: `checkpoints`
- **URL**: `https://checkpoint202605291122.blob.core.windows.net`

### Autenticación Configurada
- **Tipo**: Azure AD (RBAC)
- **Roles asignados**:
  - `Azure Event Hubs Data Owner` (en namespace)
  - `Storage Blob Data Contributor` (en storage account)
- **Usuario**: `admin@MngEnvMCAP975128.onmicrosoft.com`

## 🎯 Resultados de la Demo

### Productor (`producer_demo.py`)
✅ **Funcionando correctamente**
- Envió **200 eventos** de telemetría en 2 ejecuciones
- Cada evento incluye: device_id, temperatura, humedad, presión, timestamp
- Usa batching para optimizar throughput
- Simula 10 dispositivos IoT (device-1 a device-10)

### Consumer (`consumer_demo.py`)
✅ **Funcionando correctamente**
- Procesó **100+ eventos** exitosamente
- **Throughput alcanzado**: ~5.65 eventos/segundo
- **Temperatura promedio detectada**: 24.66°C
- **Checkpoints**: Guardados cada 10 eventos
- **Particiones procesadas**: 0, 1, 3 (distribución automática)

### Estadísticas Finales
```
Total eventos procesados: 100+
Temperatura promedio: 24.66°C
Throughput: 5.65 eventos/segundo
Tiempo transcurrido: 17.7 segundos

Eventos por dispositivo:
  device-1: 5 eventos
  device-10: 9 eventos
  device-2: 6 eventos
  device-3: 11 eventos
  device-4: 9 eventos
  device-5: 10 eventos
  device-6: 9 eventos
  device-7: 16 eventos
  device-8: 14 eventos
  device-9: 11 eventos
```

## 🚀 Cómo Ejecutar las Demos

### Prerrequisitos
```powershell
# 1. Instalar dependencias de Python
pip install azure-eventhub azure-eventhub-checkpointstoreblob-aio azure-identity

# 2. Autenticarse con Azure CLI
az login --tenant MngEnvMCAP975128.onmicrosoft.com
```

### Ejecutar Consumer (Terminal 1)
```powershell
cd C:\Users\alemartinez\src\adatabricks
python consumer_demo.py
```

El consumer mostrará:
- ✅ Conexión establecida
- 🎧 Escuchando eventos
- 📩 Eventos recibidos en tiempo real
- 📊 Estadísticas cada 10 eventos

### Ejecutar Productor (Terminal 2)
```powershell
cd C:\Users\alemartinez\src\adatabricks
python producer_demo.py
```

El productor enviará 100 eventos y mostrará progreso cada 10 eventos.

### Detener Consumer
Presiona `Ctrl+C` en la terminal del consumer para detenerlo. Mostrará estadísticas finales.

## 📂 Archivos de Demo Creados

- [`producer_demo.py`](./producer_demo.py) - Productor de eventos de telemetría
- [`consumer_demo.py`](./consumer_demo.py) - Consumer con métricas en tiempo real

## 🔧 Características Implementadas

### Productor
- ✅ Autenticación con Azure AD (DefaultAzureCredential)
- ✅ Batching de eventos para optimizar throughput
- ✅ Simulación de datos de telemetría realistas
- ✅ Generación de datos para múltiples dispositivos

### Consumer
- ✅ Autenticación con Azure AD (DefaultAzureCredential)
- ✅ Checkpoint automático cada 10 eventos
- ✅ Procesamiento de múltiples particiones
- ✅ Métricas en tiempo real:
  - Total de eventos procesados
  - Temperatura promedio
  - Throughput (eventos/segundo)
  - Distribución por dispositivo
- ✅ Manejo robusto de errores
- ✅ Estadísticas periódicas cada 10 eventos

## 🎓 Conceptos Demostrados

1. **Streaming en Tiempo Real**: Eventos fluyen del productor al consumer en tiempo real
2. **Particionamiento**: Event Hub distribuye eventos automáticamente en 4 particiones
3. **Checkpointing**: Consumer guarda progreso en Azure Blob Storage
4. **Consumer Groups**: Uso del consumer group `$Default`
5. **Autenticación Azure AD**: Uso de RBAC en lugar de connection strings
6. **Batching**: Optimización de throughput con envío de batches
7. **Métricas**: Monitoreo de throughput y latencia

## 📚 Recursos Relacionados

- [Documentación Event Hubs](../event-hubs/docs/)
- [Lab 01: Productor y Consumer básicos](../event-hubs/labs/lab-01-productor-consumer.md)
- [Lab 02: Integración con Databricks](../event-hubs/labs/lab-02-databricks-streaming.md)
- [Demo 01: Envío de Eventos](../event-hubs/demos/demo-01-envio-eventos.md)
- [Demo 02: Consumo y Visualización](../event-hubs/demos/demo-02-consumo-visualizacion.md)

## 🧹 Limpieza de Recursos (Opcional)

Para eliminar todos los recursos creados:

```powershell
# Opción 1: Eliminar solo el namespace de Event Hub y Storage Account
az eventhubs namespace delete --resource-group databricksdemos --name eh-demo-202605291122
az storage account delete --name checkpoint202605291122 --resource-group databricksdemos --yes

# Opción 2: Eliminar todo el resource group (si no contiene otros recursos importantes)
az group delete --name databricksdemos --yes
```

## ✅ Próximos Pasos

1. **Escalar el throughput**: Incrementar Throughput Units en el namespace
2. **Agregar más particiones**: Modificar el Event Hub para más paralelismo
3. **Integrar con Databricks**: Seguir el [Lab 02](../event-hubs/labs/lab-02-databricks-streaming.md)
4. **Implementar Schema Registry**: Validar estructura de eventos con Avro
5. **Monitoreo avanzado**: Configurar Azure Monitor y Application Insights

## 📊 Métricas en Azure Portal

Puedes ver métricas en tiempo real en Azure Portal:
1. Navegar a https://portal.azure.com
2. Buscar "eh-demo-202605291122"
3. En el menú izquierdo, seleccionar "Metrics"
4. Agregar métricas:
   - Incoming Messages
   - Outgoing Messages
   - Incoming Bytes
   - Throttled Requests

---

**Demo completada exitosamente** ✨

**Fecha**: 29 de mayo de 2026  
**Suscripción**: ME-MngEnvMCAP975128-alemartinez-1  
**Región**: East US
