# Módulo 2: Azure Event Hubs

## Descripción

Este módulo cubre los fundamentos de Azure Event Hubs para mensajería en tiempo real, desde la introducción hasta la integración con Databricks y AKS. Diseñado para un Knowledge Transfer (KT) de 1 sesión de 120 minutos.

## Estructura del Módulo

### 📚 Documentación Teórica (`docs/`)

1. **[Introducción a Event Hubs](docs/01-introduccion-event-hubs.md)** - Qué es, casos de uso, cuándo usarlo (45-60 min)
2. **[Conceptos Clave](docs/02-conceptos-clave.md)** - Particiones, throughput units, consumer groups (60-75 min)
3. **[Arquitectura e Integración](docs/03-arquitectura-event-hubs.md)** - Databricks, AKS, patrones (45-60 min)
4. **[Producción de Eventos](docs/04-produccion-eventos.md)** - Mejores prácticas para productores (20 min)
5. **[Consumo de Eventos](docs/05-consumo-eventos.md)** - Patrones de consumo, checkpointing (20 min)
6. **[Monitoreo y Troubleshooting](docs/06-monitoreo-troubleshooting.md)** - Métricas, alertas, diagnóstico (25 min)

### 🎬 Demos Ejecutables (`demos/`)

- **[Demo 01: Envío de Eventos](demos/demo-01-envio-eventos.md)** - Productor simple y con batching (25 min)
- **[Demo 02: Consumo y Visualización](demos/demo-02-consumo-visualizacion.md)** - Consumer con métricas en tiempo real (25 min)

### 🧪 Laboratorios Prácticos (`labs/`)

- **[Lab 01: Productor y Consumer Básicos](labs/lab-01-productor-consumer.md)** - Crear productor/consumer desde cero (30-40 min)
- **[Lab 02: Integración con Databricks](labs/lab-02-databricks-streaming.md)** - Structured Streaming + Delta Lake (45-50 min)

---

## Características Técnicas

- **Python 3.11+** con azure-eventhub 5.11+
- **Checkpointing**: Azure Blob Storage
- **Autenticación**: Managed Identity (recomendado)
- **Demos Reales**: Código ejecutable sin dependencias externas

---

## Agenda del KT (120 minutos)

| Tiempo | Sección | Duración |
|--------|---------|----------|
| 0:00-0:20 | Introducción a Event Hubs y mensajería en tiempo real | 20 min |
| 0:20-0:40 | Conceptos clave: particiones, throughput, consumer groups | 20 min |
| 0:40-1:00 | Arquitectura y encaje en solución (Databricks, AKS) | 20 min |
| 1:00-1:25 | **Demo 1**: Envío de eventos a Event Hub | 25 min |
| 1:25-1:50 | **Demo 2**: Consumo de eventos y métricas | 25 min |
| 1:50-2:00 | Q&A y checklist para KT con partner | 10 min |

---

## Requisitos Previos

- Suscripción de Azure con permisos de Contributor
- Azure CLI instalado
- Python 3.11+ instalado
- Conocimientos básicos de Python y mensajería async

---

## Cómo Usar Este Módulo

1. **Antes del KT**: Leer documentos 01-03 (contexto general)
2. **Durante el KT**: Ejecutar demos 01 y 02 en vivo
3. **Después del KT**: Completar labs 01-02 para práctica
4. **Al recibir KT del partner**: Usar checklist de preguntas clave

---

## Tiempo Total Estimado

- **Documentación**: ~4-5 horas de lectura
- **Demos**: ~1 hora de ejecución
- **Labs**: ~1.5 horas de práctica
- **Total**: ~6-7 horas (incluye sesión de 2 horas)

---

## Recursos Adicionales

Ver carpeta `../resources/` para:
- Checklist de preguntas para KT
- FAQ de Event Hubs
- Glosario de términos
- Referencias externas

---

## Integración con Módulo Databricks

Los eventos enviados a Event Hubs pueden procesarse con Databricks Structured Streaming:

```python
# En Databricks
df = (spark
  .readStream
  .format("eventhubs")
  .options(**ehConf)
  .load()
)
```

Ver [Integración con Databricks](docs/03-arquitectura-event-hubs.md) para detalles completos.
