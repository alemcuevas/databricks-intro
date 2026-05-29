# Azure Data Platform Training - Knowledge Transfer

Este repositorio contiene material completo de Knowledge Transfer (KT) para **Azure Databricks** y **Azure Event Hubs**, diseñado para facilitar la transferencia de conocimiento de soluciones de datos en Azure.

---

## 📚 Contenido del Repositorio

### [Módulo 1: Azure Databricks](databricks/)

Material completo para aprender Azure Databricks desde cero hasta integración avanzada.

**Incluye**:
- 6 documentos teóricos (~45,000 palabras)
- 6 laboratorios prácticos con notebooks ejecutables
- 2 demos end-to-end
- Arquitectura Medallion (Bronze → Silver → Gold)
- Integración con servicios de Azure

**Duración**: 6 sesiones de 2 horas (12 horas total)

👉 **[Ver módulo completo de Databricks](databricks/README.md)**

---

### [Módulo 2: Azure Event Hubs](event-hubs/)

Material completo para Event Hubs y streaming de eventos en tiempo real.

**Incluye**:
- 6 documentos teóricos sobre Event Hubs
- 2 demos ejecutables (productor + consumer)
- 2 laboratorios prácticos
- Integración con Databricks y AKS
- Patrones de arquitectura event-driven

**Duración**: 1 sesión de 2 horas

👉 **[Ver módulo completo de Event Hubs](event-hubs/README.md)**

---

## 🎯 Objetivos del Training

Este material está diseñado para:

✅ **Transferencia de conocimiento** de partner a equipo interno  
✅ **Onboarding** de nuevos miembros del equipo  
✅ **Referencia técnica** para consultas rápidas  
✅ **Laboratorios prácticos** para aprender haciendo  

---

## 🏗️ Estructura del Repositorio

```
azure-data-platform-kt/
├── databricks/                    # Módulo 1: Databricks
│   ├── docs/                      # 6 documentos teóricos
│   ├── labs/                      # 6 guías de laboratorios
│   ├── demos/                     # 2 demos end-to-end
│   ├── notebooks/                 # 6 notebooks ejecutables
│   └── README.md
│
├── event-hubs/                    # Módulo 2: Event Hubs
│   ├── docs/                      # 6 documentos teóricos
│   ├── demos/                     # 2 demos con código ejecutable
│   ├── labs/                      # 2 laboratorios prácticos
│   └── README.md
│
└── resources/                     # Recursos compartidos
    ├── checklist-kt.md            # Checklist para KT de Databricks
    ├── checklist-kt-eventhubs.md  # Checklist para KT de Event Hubs
    ├── faq.md                     # FAQ general
    ├── glosario.md                # Glosario de términos
    └── referencias.md             # Links y recursos externos
```

---

## 🚀 Quick Start

### Para Databricks

1. Importar notebooks a tu workspace de Databricks
2. Crear cluster (Standard_DS3_v2, 2-10 workers, autoscaling)
3. Ejecutar notebooks en orden: lab-01 → lab-06
4. Seguir documentación en `databricks/docs/`

```bash
# Subir notebooks con Databricks CLI
databricks workspace import-dir databricks/notebooks/ /Shared/azure-training/databricks/
```

### Para Event Hubs

1. Crear Event Hub namespace en Azure
2. Instalar dependencias: `pip install azure-eventhub azure-eventhub-checkpointstoreblob-aio`
3. Ejecutar demos en `event-hubs/demos/`
4. Completar labs para práctica

---

## 📋 Requisitos Previos

### General
- Suscripción de Azure con permisos de Contributor
- Azure CLI instalado
- Python 3.11+ (para Event Hubs)
- Conocimientos básicos de SQL y Python

### Databricks Específico
- Workspace de Azure Databricks (Premium recomendado)
- Acceso para crear clusters

### Event Hubs Específico
- Event Hub namespace
- Storage Account (para checkpoints)

---

## 🎓 Rutas de Aprendizaje

### Ruta 1: Data Engineer (Principiante → Intermedio)

**Semanas 1-2**: Databricks Básico
1. Introducción y componentes clave
2. Clusters y costos
3. Notebooks básicos
4. Transformaciones con DataFrames

**Semana 3**: Databricks Intermedio
5. Jobs y workflows
6. Integración de datos

**Semana 4**: Event Hubs y Streaming
7. Conceptos de Event Hubs
8. Productor y consumer
9. Integración Databricks + Event Hubs

**Duración Total**: 4 semanas (~20 horas)

---

### Ruta 2: Solution Architect (Conceptual)

**Día 1**: Arquitecturas de Referencia (4 horas)
- `databricks/docs/03-arquitectura-integracion.md`
- `event-hubs/docs/03-arquitectura-event-hubs.md`
- Medallion Architecture
- Lambda/Kappa patterns

**Día 2**: Costos y Operaciones (4 horas)
- `databricks/docs/04-clusters-costos.md`
- `event-hubs/docs/02-conceptos-clave.md` (Throughput Units)
- `event-hubs/docs/06-monitoreo-troubleshooting.md`

**Duración Total**: 2 días (~8 horas)

---

### Ruta 3: Platform Engineer (Operaciones)

**Semana 1**: Operaciones Databricks
- Clusters autoscaling
- Monitoreo y troubleshooting
- Unity Catalog (governance)

**Semana 2**: Operaciones Event Hubs
- Throughput planning
- Checkpointing y consumer groups
- Alertas y métricas
- KEDA para auto-scaling en AKS

**Duración Total**: 2 semanas (~12 horas)

---

## 🤝 Cómo Usar Este Repo para Knowledge Transfer

### Antes del KT con el Partner

1. **Revisar checklists**:
   - `resources/checklist-kt.md` (Databricks)
   - `resources/checklist-kt-eventhubs.md` (Event Hubs)

2. **Preparar preguntas específicas** basadas en tu arquitectura

3. **Leer documentos de introducción**:
   - `databricks/docs/01-introduccion-databricks.md`
   - `event-hubs/docs/01-introduccion-event-hubs.md`

### Durante el KT

- Tomar notas en secciones "Preguntas para el KT" de cada documento
- Pedir al partner que explique decisiones de arquitectura específicas
- Solicitar acceso a recursos (workspaces, Event Hubs, dashboards)

### Después del KT

1. Completar laboratorios prácticos
2. Documentar gaps de conocimiento
3. Crear runbooks operacionales específicos de tu implementación

---

## 📖 Documentación Destacada

### Conceptos Clave

- **[¿Qué es Databricks?](databricks/docs/01-introduccion-databricks.md)** - Introducción completa
- **[Arquitectura Medallion](databricks/docs/03-arquitectura-integracion.md)** - Bronze/Silver/Gold
- **[¿Qué es Event Hubs?](event-hubs/docs/01-introduccion-event-hubs.md)** - Streaming 101
- **[Particiones y Throughput](event-hubs/docs/02-conceptos-clave.md)** - Conceptos críticos

### Guías Prácticas

- **[Optimización de Costos en Databricks](databricks/docs/04-clusters-costos.md)**
- **[Patrones de Productor/Consumer](event-hubs/docs/04-produccion-eventos.md)**
- **[Troubleshooting de Event Hubs](event-hubs/docs/06-monitoreo-troubleshooting.md)**

---

## 🔧 Herramientas Utilizadas

- **Azure Databricks**: Plataforma de análisis basada en Apache Spark
- **Azure Event Hubs**: Servicio de ingesta de eventos en tiempo real
- **Delta Lake**: Storage layer con ACID transactions
- **PySpark**: API de Python para Spark
- **Python azure-eventhub SDK**: Cliente oficial de Event Hubs

---

## 📝 Convenciones de Código

Todo el código sigue estas convenciones:

- **Comentarios inline en español** (qué/por qué/cómo)
- **Managed tables** en Databricks (sin Azure Storage externo)
- **Managed Identity** para autenticación (recomendado)
- **Ejemplos ejecutables** sin dependencias complejas

---

## 🤝 Contribuciones

Este es un repositorio de training interno. Para mejoras o correcciones:

1. Crear branch: `git checkout -b mejora/descripcion`
2. Hacer cambios
3. Commit: `git commit -m "Descripción clara del cambio"`
4. Push: `git push origin mejora/descripcion`
5. Crear Pull Request

---

## 📄 Licencia

Material interno de training. No distribuir fuera de la organización.

---

## 👥 Contacto y Soporte

Para preguntas sobre el material:
- **Databricks**: Ver FAQ en `resources/faq.md`
- **Event Hubs**: Ver FAQ en `resources/faq.md`
- **Arquitectura**: Consultar documentos en `docs/` de cada módulo

---

## 🗓️ Últimas Actualizaciones

- **2024-05-29**: Agregado módulo completo de Event Hubs
- **2024-05-29**: Reorganización de estructura de repositorio
- **2024-05-28**: Módulo de Databricks completado
- **2024-05-27**: Notebooks con inline comments en español
- **2024-05-26**: Repositorio inicial

---

**¡Comienza tu aprendizaje!** 👇

- 📘 [Módulo Databricks →](databricks/README.md)
- 📙 [Módulo Event Hubs →](event-hubs/README.md)
- 📝 [Recursos y Checklists →](resources/)
