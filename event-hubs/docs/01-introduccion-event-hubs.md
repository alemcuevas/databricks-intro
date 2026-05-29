# Introducción a Azure Event Hubs

## Índice

1. [¿Qué es Azure Event Hubs?](#qué-es-azure-event-hubs)
2. [Casos de Uso Principales](#casos-de-uso-principales)
3. [Arquitectura de Mensajería](#arquitectura-de-mensajería)
4. [Event Hubs vs Otras Soluciones](#event-hubs-vs-otras-soluciones)
5. [Ventajas y Limitaciones](#ventajas-y-limitaciones)
6. [Cuándo Usar Event Hubs](#cuándo-usar-event-hubs)

---

## ¿Qué es Azure Event Hubs?

Azure Event Hubs es un servicio de ingesta de datos en tiempo real completamente administrado, capaz de recibir y procesar millones de eventos por segundo. Funciona como una "puerta de entrada" para pipelines de datos, donde múltiples productores envían eventos que luego son consumidos por múltiples aplicaciones downstream.

### Características Principales

**Ingesta Masiva**
- Capacidad de procesar millones de eventos por segundo
- Retención configurable de 1 a 90 días
- Protocolo AMQP 1.0 y HTTPS para envío de eventos

**Escalabilidad Elástica**
- Auto-inflate para ajustar throughput units automáticamente
- Particionamiento para distribución de carga
- Sin gestión de infraestructura

**Integración Nativa**
- Conectores para Azure Stream Analytics, Databricks, Functions
- Compatibilidad con Apache Kafka (Event Hubs como Kafka endpoint)
- SDKs para .NET, Java, Python, JavaScript, Go

**Durabilidad y Confiabilidad**
- Almacenamiento replicado en zonas de disponibilidad
- Checkpointing para garantizar procesamiento at-least-once
- Recuperación ante fallos con consumer groups

### Analogía del Mundo Real

Imagina Event Hubs como una **estación de tren central**:

- **Productores** = Trenes que llegan de diferentes ciudades con pasajeros
- **Event Hub** = Estación con múltiples andenes (particiones)
- **Eventos** = Pasajeros con información (datos)
- **Consumidores** = Autobuses que recogen pasajeros y los llevan a destinos específicos
- **Consumer Groups** = Diferentes líneas de autobuses que pueden recoger a los mismos pasajeros

Cada "tren" (productor) deposita "pasajeros" (eventos) en "andenes" (particiones), y múltiples "líneas de autobuses" (consumer groups) pueden recoger copias de esos "pasajeros" para llevarlos a diferentes destinos (aplicaciones).

---

## Casos de Uso Principales

### 1. Telemetría de IoT

**Escenario**: Fábrica con 10,000 sensores enviando métricas cada segundo

```
[Sensores IoT] → Event Hub → [Stream Analytics] → [Alertas + Databricks]
                                                    ↓
                                                [Power BI]
```

**Beneficios**:
- Ingesta de 10 millones de eventos/seg
- Detección de anomalías en tiempo real
- Dashboards de métricas operacionales

**Ejemplo Real**: Empresa manufacturera detecta vibraciones anormales en maquinaria y genera orden de mantenimiento preventivo automáticamente.

### 2. Monitoreo de Aplicaciones

**Escenario**: Aplicación web con logs de 1,000 instancias

```
[App Logs] → Event Hub → [Azure Functions] → [Log Analytics]
                         ↓
                      [Alertas de Errores]
```

**Beneficios**:
- Centralización de logs distribuidos
- Análisis de errores en tiempo real
- Trazabilidad de transacciones

**Ejemplo Real**: E-commerce detecta picos de errores 500 y escala instancias automáticamente.

### 3. Análisis de Clickstream

**Escenario**: Sitio web con millones de usuarios navegando

```
[Website] → Event Hub → [Databricks] → [ML Model] → [Recomendaciones]
                        ↓
                    [Data Lake (histórico)]
```

**Beneficios**:
- Captura de comportamiento de usuario en tiempo real
- Personalización inmediata
- Análisis de patrones de navegación

**Ejemplo Real**: Plataforma de streaming recomienda contenido basado en clics de los últimos 5 minutos.

### 4. Procesamiento de Transacciones Financieras

**Escenario**: Sistema bancario procesando pagos

```
[ATMs + Apps] → Event Hub → [Fraud Detection] → [Aprobación/Rechazo]
                            ↓
                         [Audit Trail]
```

**Beneficios**:
- Latencia ultra-baja (<100ms)
- Detección de fraude en tiempo real
- Cumplimiento regulatorio (audit trail completo)

**Ejemplo Real**: Banco detecta patrón sospechoso de transacciones y bloquea tarjeta en 50ms.

### 5. Sincronización de Datos en Tiempo Real

**Escenario**: Arquitectura de microservicios con event-driven design

```
[Order Service] → Event Hub → [Inventory Service]
                             → [Notification Service]
                             → [Analytics Service]
```

**Beneficios**:
- Desacoplamiento de servicios
- Broadcast de eventos a múltiples consumidores
- Replay de eventos para recuperación

**Ejemplo Real**: E-commerce actualiza inventario, envía email y registra analítica desde un solo evento "OrderCreated".

---

## Arquitectura de Mensajería

### Modelo de Publicación/Suscripción

Event Hubs implementa un modelo **pub/sub** distribuido:

```
┌─────────────┐
│ Producer 1  │──┐
└─────────────┘  │
                 ├──→ ┌───────────────┐     ┌──────────────┐
┌─────────────┐  │    │               │     │ Consumer     │
│ Producer 2  │──┼───→│  EVENT HUB    │────→│ Group A      │
└─────────────┘  │    │               │     └──────────────┘
                 ├──→ │ (Partitions)  │     ┌──────────────┐
┌─────────────┐  │    │               │────→│ Consumer     │
│ Producer N  │──┘    └───────────────┘     │ Group B      │
└─────────────┘                             └──────────────┘
```

**Características del Modelo**:

1. **Múltiples Productores**: Cualquier aplicación puede enviar eventos
2. **Múltiples Consumidores**: Cada consumer group lee independientemente
3. **Desacoplamiento**: Productores y consumidores no se conocen entre sí
4. **Escalabilidad**: Agregar consumidores no afecta productores

### Flujo de un Evento

```
1. PRODUCCIÓN
   Producer serializa evento → Envía a Event Hub → ACK de confirmación

2. ALMACENAMIENTO
   Event Hub escribe en partición → Replica en zona → Asigna offset

3. CONSUMO
   Consumer lee desde offset → Procesa evento → Actualiza checkpoint
```

**Tiempo de vida del evento**:
- Retention: 1-90 días (configurable)
- Después de retention: evento eliminado automáticamente
- Consumer puede leer eventos antiguos usando offset

### Garantías de Entrega

Event Hubs proporciona **at-least-once delivery**:

**Escenario Normal**:
```
Producer → Event Hub → Consumer (procesa 1 vez) ✓
```

**Escenario con Fallo**:
```
Producer → Event Hub → Consumer (falla antes de checkpoint)
                    → Consumer (reintenta, procesa 2 veces) ⚠️
```

**Implicación**: Los consumidores deben ser **idempotentes** o usar deduplicación.

---

## Event Hubs vs Otras Soluciones

### Comparación con Service Bus

| Característica | Event Hubs | Service Bus |
|----------------|------------|-------------|
| **Propósito** | Streaming de eventos masivos | Mensajería empresarial |
| **Throughput** | Millones de eventos/seg | Miles de mensajes/seg |
| **Ordenamiento** | Por partición | Por sesión (opcional) |
| **Retención** | 1-90 días | 14 días máx |
| **Patrón** | Pub/Sub con replay | Queue + Topic/Subscription |
| **Casos de uso** | Telemetría, logs, clickstream | Transacciones, comandos, workflows |
| **Precio** | Por throughput unit (~$22/mes) | Por operaciones (~$10/millón) |

**Cuándo usar cada uno**:
- **Event Hubs**: Volumen alto, análisis histórico, múltiples consumidores
- **Service Bus**: Mensajería confiable, dead-lettering, transacciones

### Comparación con Apache Kafka

| Característica | Event Hubs | Kafka (self-hosted) |
|----------------|------------|---------------------|
| **Gestión** | Fully managed PaaS | Requiere administración |
| **Costo inicial** | $0 + uso | Infraestructura + personal |
| **Escalabilidad** | Auto-inflate | Manual (agregar brokers) |
| **Compatibilidad** | Endpoint Kafka incluido | Nativo |
| **Integración Azure** | Nativa (Functions, Databricks) | Requiere conectores |
| **SLA** | 99.95% | Depende de implementación |

**Cuándo usar cada uno**:
- **Event Hubs**: Teams pequeños, preferencia por managed services, integración Azure
- **Kafka**: Control total, multi-cloud, equipo con expertise en Kafka

### Comparación con Storage Queue

| Característica | Event Hubs | Storage Queue |
|----------------|------------|---------------|
| **Latencia** | <10ms | ~50-100ms |
| **Tamaño mensaje** | 1MB | 64KB |
| **Ordenamiento** | Garantizado por partición | No garantizado |
| **Throughput** | Millones/seg | Miles/seg |
| **Casos de uso** | Streaming en tiempo real | Colas de trabajo async |
| **Precio** | Por throughput unit | Por operación + storage |

---

## Ventajas y Limitaciones

### ✅ Ventajas

**1. Escalabilidad Sin Límites**
- Auto-inflate ajusta throughput units automáticamente
- Soporte para Premium (hasta 400 throughput units)
- Particionamiento horizontal

**2. Fully Managed**
- Zero administración de infraestructura
- Actualizaciones automáticas
- Replicación en zonas de disponibilidad

**3. Integración con Ecosistema Azure**
- Event Grid para routing avanzado
- Stream Analytics para procesamiento sin código
- Databricks para análisis con Spark
- Azure Functions para procesamiento serverless

**4. Compatibilidad Kafka**
- Mismo protocolo, sin cambios de código
- Migración gradual desde Kafka on-premise
- Ahorro de costos de gestión

**5. Captura Automática**
- Archivado automático a Azure Storage/Data Lake
- Formato Avro para compresión eficiente
- Sin código necesario

**6. Seguridad Empresarial**
- Managed Identity para autenticación
- Private Link para tráfico privado
- Customer-managed keys (CMK) para cifrado

### ⚠️ Limitaciones

**1. No es Base de Datos**
- No hay queries complejos (solo lectura secuencial)
- No hay índices
- Retención máxima de 90 días

**2. Ordenamiento Limitado**
- Solo garantizado dentro de una partición
- Orden global requiere single partition (limita throughput)

**3. Curva de Aprendizaje**
- Conceptos de partitioning requieren diseño cuidadoso
- Consumer groups y checkpointing no triviales
- Tuning de throughput units puede ser complejo

**4. Costo en Escenarios de Bajo Volumen**
- Minimum billable: 1 throughput unit (~$22/mes) aunque no se use
- Para <1000 eventos/seg, Storage Queue puede ser más económico

**5. Sin Dead Letter Queue Nativo**
- Requiere implementación manual (enviar a otro Event Hub)
- Service Bus tiene DLQ built-in

---

## Cuándo Usar Event Hubs

### ✅ Usar Event Hubs Cuando...

**Volumen Alto**
- Más de 10,000 eventos por segundo
- Picos de tráfico impredecibles
- Crecimiento exponencial esperado

**Múltiples Consumidores**
- 2+ aplicaciones leyendo mismos eventos
- Necesidad de replay histórico
- Arquitectura event-driven

**Integración con Azure**
- Ya usas Databricks, Stream Analytics, Functions
- Necesitas integración con Data Lake
- Prefieres managed services

**Baja Latencia Requerida**
- Procesamiento en <100ms
- Decisiones en tiempo real
- Telemetría IoT

**Compatibilidad Kafka**
- Migración desde Kafka on-premise
- Equipo familiarizado con Kafka APIs
- Multi-cloud con endpoint Kafka

### ❌ NO Usar Event Hubs Cuando...

**Volumen Muy Bajo**
- <1000 eventos/día
- Tráfico esporádico
- Costo de 1 TU no justificado → Usar Storage Queue

**Necesitas Transacciones**
- Operaciones ACID requeridas
- Rollback de mensajes
- Orden global estricto → Usar Service Bus

**Queries Complejas**
- Búsquedas por campos específicos
- Agregaciones complejas
- Índices necesarios → Usar Cosmos DB + Change Feed

**Procesamiento Síncrono**
- Request/response pattern
- Llamadas HTTP directas suficientes
- Sin necesidad de desacoplamiento → Usar API Management

---

## Conceptos Clave para el KT

Al recibir el Knowledge Transfer del partner, asegúrate de entender:

### 🎯 Diseño de Arquitectura

1. **¿Cuál es la fuente de eventos?**
   - IoT devices, aplicaciones web, microservicios, logs
   - Frecuencia y volumen esperado
   - Formato de eventos (JSON, Avro, Protobuf)

2. **¿Cómo se particionan los eventos?**
   - Partition key strategy
   - Número de particiones configurado
   - Impacto en ordenamiento

3. **¿Quiénes son los consumidores?**
   - Databricks, Stream Analytics, Functions, custom apps
   - Consumer groups definidos
   - Latencia requerida

### 📊 Throughput y Costos

4. **¿Cuántos throughput units están asignados?**
   - TUs estándar vs Premium
   - Auto-inflate configurado
   - Monitoreo de throttling

5. **¿Cuál es el costo mensual actual?**
   - Por throughput units
   - Por captura (si aplica)
   - Por almacenamiento de eventos

### 🔒 Seguridad y Compliance

6. **¿Cómo se autentican productores y consumidores?**
   - Shared Access Signatures (SAS)
   - Managed Identity (recomendado)
   - Azure AD

7. **¿Hay private endpoints configurados?**
   - Tráfico privado vía VNET
   - IP firewall rules
   - Restricciones de acceso

### 🔧 Operaciones y Monitoreo

8. **¿Cómo se monitorea el Event Hub?**
   - Métricas clave (incoming/outgoing messages, throttled requests)
   - Alertas configuradas
   - Diagnóstico de errores

9. **¿Hay captura automática?**
   - Destino (Storage/Data Lake)
   - Formato (Avro)
   - Retención configurada

10. **¿Cuál es el plan de recuperación ante desastres?**
    - Geo-disaster recovery configurado
    - Namespace secundario
    - Procedimiento de failover

---

## Próximos Pasos

Ahora que comprendes **qué es Event Hubs** y **cuándo usarlo**, los siguientes documentos profundizan en:

- **[02-conceptos-clave.md](02-conceptos-clave.md)**: Particiones, throughput units, consumer groups
- **[03-arquitectura-event-hubs.md](03-arquitectura-event-hubs.md)**: Cómo integrar con Databricks y AKS
- **[04-produccion-eventos.md](04-produccion-eventos.md)**: Mejores prácticas para productores
- **[05-consumo-eventos.md](05-consumo-eventos.md)**: Patrones de consumo y checkpointing
- **[06-monitoreo-troubleshooting.md](06-monitoreo-troubleshooting.md)**: Operaciones y optimización

**Tiempo estimado de lectura**: 45-60 minutos
