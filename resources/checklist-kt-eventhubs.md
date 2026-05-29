# Checklist para Knowledge Transfer de Event Hubs

## Antes del KT con el Partner

### Preparación General
- [ ] Leer documentación de introducción: `event-hubs/docs/01-introduccion-event-hubs.md`
- [ ] Revisar conceptos clave: particiones, throughput units, consumer groups
- [ ] Tener acceso al Azure Portal con permisos de lectura en Event Hub namespace
- [ ] Tener acceso a documentación actual del proyecto

### Información a Obtener Antes
- [ ] Nombre del Event Hub namespace
- [ ] Nombres de los Event Hubs en uso
- [ ] Diagramas de arquitectura actuales
- [ ] Documentación de esquemas de eventos (si existe)

---

## Durante el KT: Preguntas Críticas

### 1. Arquitectura y Diseño

#### Event Hub Namespace
- [ ] **¿Cuántos Event Hub namespaces hay en la solución?**
  - Nombres y propósitos de cada uno
  - Tier (Standard/Premium/Dedicated)
  - Región y configuración de redundancia

- [ ] **¿Cuántos Event Hubs hay dentro de cada namespace?**
  - Nombres y propósitos
  - Ejemplos de eventos que maneja cada uno

#### Particionamiento
- [ ] **¿Cuántas particiones tiene cada Event Hub?**
  - Razón de esta cantidad
  - Cómo se distribuyen los eventos (partition key)

- [ ] **¿Qué partition key strategy se usa?**
  - Por usuario, dispositivo, sesión, etc.
  - ¿Hay riesgo de hotspots (particiones sobrecargadas)?

#### Formato de Eventos
- [ ] **¿Qué formato tienen los eventos?**
  - JSON, Avro, Protobuf, etc.
  - ¿Hay schema registry configurado?

- [ ] **¿Cuál es el tamaño promedio de un evento?**
  - En bytes
  - Impacto en cálculo de throughput units

### 2. Productores

#### Fuentes de Eventos
- [ ] **¿Quiénes son los productores?**
  - IoT devices, aplicaciones web, microservicios, etc.
  - ¿Cuántos productores hay?

- [ ] **¿Dónde está el código de los productores?**
  - Repositorio
  - Lenguaje (Python, .NET, Java, etc.)

- [ ] **¿Qué volumen de eventos generan?**
  - Eventos por segundo promedio
  - Picos máximos esperados
  - Patrones de tráfico (horarios, días de semana vs. fin de semana)

#### Configuración de Productores
- [ ] **¿Cómo se autentican los productores?**
  - Connection string (menos seguro)
  - Managed Identity (recomendado)
  - SAS tokens

- [ ] **¿Usan batching?**
  - Tamaño de batches
  - Estrategia de retry

- [ ] **¿Hay manejo de errores?**
  - Qué pasa si Event Hub rechaza eventos (throttling)
  - ¿Hay dead letter queue o logs de errores?

### 3. Consumidores

#### Aplicaciones Consumidoras
- [ ] **¿Quiénes son los consumidores?**
  - Databricks, Stream Analytics, Azure Functions, custom apps, etc.
  - ¿Cuántos consumer groups hay?

- [ ] **¿Dónde está el código de los consumidores?**
  - Repositorio
  - Lenguaje/framework

- [ ] **¿Qué hacen con los eventos?**
  - Guardar en base de datos
  - Enviar alertas
  - Transformaciones y agregaciones
  - Archivado

#### Checkpointing
- [ ] **¿Dónde se guardan los checkpoints?**
  - Storage Account (nombre)
  - Container

- [ ] **¿Con qué frecuencia se hace checkpoint?**
  - Cada N eventos
  - Cada X segundos

- [ ] **¿Qué pasa si un consumer falla?**
  - Proceso de recuperación
  - SLA de latencia de procesamiento

#### Escalado
- [ ] **¿Cómo escalan los consumidores?**
  - Múltiples instancias
  - KEDA en AKS (si aplica)
  - Configuración de auto-scaling

- [ ] **¿Hay lag de consumo?**
  - Cómo se monitorea
  - Qué alertas hay configuradas

### 4. Throughput y Costos

#### Throughput Units
- [ ] **¿Cuántos throughput units están asignados?**
  - Por namespace
  - ¿Está habilitado auto-inflate?
  - Límite máximo de auto-inflate

- [ ] **¿Cuál es el costo mensual actual?**
  - Por throughput units
  - Por ingress/egress
  - Costo total del servicio

- [ ] **¿Ha habido problemas de throttling?**
  - Frecuencia
  - Cómo se resolvieron
  - Métricas de throttling en los últimos 30 días

#### Retención
- [ ] **¿Cuántos días de retención están configurados?**
  - Por Event Hub
  - Razón de esta duración

- [ ] **¿Se usa Event Hubs Capture?**
  - Destino (Blob Storage / Data Lake)
  - Formato (Avro)
  - Uso del archivo histórico

### 5. Seguridad y Compliance

#### Autenticación y Autorización
- [ ] **¿Cómo se controla el acceso?**
  - Shared Access Signatures (SAS)
  - Managed Identities
  - Azure AD

- [ ] **¿Hay private endpoints configurados?**
  - VNET integration
  - IP firewall rules

- [ ] **¿Se usan customer-managed keys (CMK)?**
  - Para cifrado en reposo
  - Key Vault usado

#### Compliance
- [ ] **¿Hay requisitos de compliance?**
  - GDPR, HIPAA, PCI-DSS, etc.
  - Retención de audit logs

- [ ] **¿Qué datos sensibles viajan por Event Hub?**
  - PII (personally identifiable information)
  - ¿Se enmascaran o cifran?

### 6. Integración con Otros Servicios

#### Databricks
- [ ] **¿Se consumen eventos con Databricks?**
  - Structured Streaming o batch
  - Ubicación de notebooks
  - Frecuencia de procesamiento

- [ ] **¿Dónde se guardan los resultados?**
  - Delta Lake
  - Cosmos DB
  - Azure SQL
  - Otro destino

#### Stream Analytics
- [ ] **¿Hay jobs de Stream Analytics?**
  - Nombres y propósitos
  - Queries configuradas
  - Outputs (destinos)

#### Azure Functions
- [ ] **¿Hay Functions consumiendo eventos?**
  - Trigger de Event Hub configurado
  - ¿Qué procesamiento realizan?

#### AKS
- [ ] **¿Hay consumers corriendo en AKS?**
  - Deployment names
  - KEDA configurado
  - Estrategia de scaling

### 7. Monitoreo y Alertas

#### Métricas
- [ ] **¿Qué métricas se monitorean?**
  - Incoming Messages
  - Outgoing Messages
  - Throttled Requests
  - Consumer Lag

- [ ] **¿Dónde se visualizan?**
  - Azure Monitor
  - Power BI
  - Grafana
  - Application Insights

#### Alertas
- [ ] **¿Qué alertas están configuradas?**
  - Throttling
  - Consumer lag alto
  - Errores de producers/consumers

- [ ] **¿Quién recibe las alertas?**
  - Email
  - Teams/Slack
  - PagerDuty

- [ ] **¿Cuál es el procedimiento ante alertas?**
  - Runbook documentado
  - Escalación

#### Logs
- [ ] **¿Están habilitados los diagnostic logs?**
  - OperationalLogs
  - RuntimeAuditLogs
  - ArchiveLogs

- [ ] **¿Dónde se almacenan los logs?**
  - Log Analytics Workspace
  - Storage Account

### 8. Disaster Recovery

#### Geo-Disaster Recovery
- [ ] **¿Está configurado geo-disaster recovery?**
  - Namespace secundario
  - Región secundaria
  - Alias de failover

- [ ] **¿Cuál es el RTO (Recovery Time Objective)?**
  - Tiempo objetivo para recuperación

- [ ] **¿Cuál es el RPO (Recovery Point Objective)?**
  - Cuántos eventos se pueden perder

- [ ] **¿Se ha probado el failover?**
  - Fecha del último test
  - Resultados

#### Backups
- [ ] **¿Se respaldan los eventos?**
  - Event Hubs Capture
  - Destino de archivos

- [ ] **¿Hay procedimiento para replay de eventos?**
  - Desde qué offset/timestamp
  - Cómo se ejecuta

### 9. Operaciones del Día a Día

#### Mantenimiento
- [ ] **¿Hay ventanas de mantenimiento?**
  - Frecuencia
  - Impacto en productores/consumidores

- [ ] **¿Cómo se actualizan los Event Hubs?**
  - Cambio de particiones
  - Cambio de throughput units
  - Cambio de retención

#### Troubleshooting
- [ ] **¿Cuáles son los problemas más comunes?**
  - Throttling
  - Consumer lag
  - Eventos perdidos

- [ ] **¿Cómo se diagnostican?**
  - Herramientas
  - Queries en Log Analytics
  - Proceso paso a paso

- [ ] **¿Hay documentación operacional?**
  - Runbooks
  - Procedimientos de troubleshooting
  - Contactos de escalación

### 10. Documentación y Conocimiento

#### Documentación Técnica
- [ ] **¿Dónde está la documentación?**
  - Confluence, SharePoint, Git, etc.
  - Diagramas de arquitectura
  - Esquemas de eventos

- [ ] **¿Está actualizada?**
  - Fecha de última actualización
  - Responsable de mantenerla

#### Conocimiento del Equipo
- [ ] **¿Quién más conoce Event Hubs en el equipo?**
  - Contactos
  - Nivel de expertise

- [ ] **¿Hay training previo?**
  - Material de onboarding
  - Tutoriales internos

---

## Después del KT

### Validación
- [ ] Ejecutar demos de `event-hubs/demos/` para entender flujo completo
- [ ] Completar labs de `event-hubs/labs/` para práctica hands-on
- [ ] Revisar código de productores y consumidores en repositorios
- [ ] Validar acceso a todos los recursos mencionados

### Documentación
- [ ] Crear runbook operacional específico de tu proyecto
- [ ] Documentar decisiones de arquitectura no obvias
- [ ] Actualizar diagramas de arquitectura si es necesario
- [ ] Crear FAQ interna con preguntas específicas del equipo

### Seguimiento
- [ ] Agendar sesión de Q&A follow-up con el partner (2-4 semanas después)
- [ ] Identificar gaps de conocimiento
- [ ] Solicitar acceso a recursos faltantes

---

## Recursos de Referencia

- [Introducción a Event Hubs](../event-hubs/docs/01-introduccion-event-hubs.md)
- [Conceptos Clave](../event-hubs/docs/02-conceptos-clave.md)
- [Arquitectura e Integración](../event-hubs/docs/03-arquitectura-event-hubs.md)
- [Monitoreo y Troubleshooting](../event-hubs/docs/06-monitoreo-troubleshooting.md)

---

**Nota**: Este checklist es una guía completa. No todas las preguntas aplican a todos los proyectos. Usa las secciones relevantes para tu caso específico.
