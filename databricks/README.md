# Módulo 1: Azure Databricks

## Descripción

Este módulo cubre los fundamentos de Azure Databricks, desde la introducción hasta la integración con servicios de Azure. Está diseñado para un Knowledge Transfer (KT) de 6 sesiones, cada una de 120 minutos.

## Estructura del Módulo

### 📚 Documentación Teórica (`docs/`)

1. **[Introducción a Databricks](docs/01-introduccion-databricks.md)** - Qué es, arquitectura, casos de uso
2. **[Componentes Clave](docs/02-componentes-clave.md)** - Clusters, notebooks, jobs, workspace
3. **[Arquitectura e Integración](docs/03-arquitectura-integracion.md)** - Medallion, lakehouse, Unity Catalog
4. **[Clusters y Costos](docs/04-clusters-costos.md)** - SKUs, pricing, optimización
5. **[Notebooks y Jobs](docs/05-notebooks-jobs.md)** - Patrones de producción, orquestación
6. **[Integración Azure](docs/06-integracion-azure.md)** - Storage, Synapse, Data Factory

### 🧪 Laboratorios Prácticos (`labs/`)

1. **[Lab 01: Configuración Inicial](labs/lab-01-configuracion-inicial.md)** - Exploración del workspace (30 min)
2. **[Lab 02: Clusters](labs/lab-02-clusters.md)** - Autoscaling y calculadora de costos (30-40 min)
3. **[Lab 03: Notebooks](labs/lab-03-notebooks.md)** - Magic commands, widgets (35-45 min)
4. **[Lab 04: Transformación de Datos](labs/lab-04-transformacion-datos.md)** - Medallion architecture (45-50 min)
5. **[Lab 05: Jobs](labs/lab-05-jobs.md)** - Workflows multi-tarea (35-45 min)
6. **[Lab 06: Integración de Datos](labs/lab-06-integracion-datos.md)** - Formatos múltiples (45-50 min)

### 💻 Notebooks Ejecutables (`notebooks/`)

- `lab-01-workspace.ipynb` - DataFrames, transformaciones básicas, Delta Lake
- `lab-02-clusters.ipynb` - Tests de carga, calculadora de costos
- `lab-03-notebooks.ipynb` - Features avanzadas, widgets, optimización
- `lab-04-transformacion.ipynb` - Implementación completa de Medallion
- `lab-05-jobs.ipynb` - Notebooks parametrizables para workflows
- `lab-06-integracion.ipynb` - JOINs, streaming con Auto Loader

### 🎬 Demos (`demos/`)

- **[Demo 01: Notebook de Transformación](demos/demo-01-notebook-transformacion.md)** - Transformación end-to-end
- **[Demo 02: Ejecución de Job](demos/demo-02-job-ejecucion.md)** - Configuración y monitoreo

## Características Técnicas

- **PySpark 3.5** con Delta Lake 2.4+
- **Managed Tables**: Uso de `saveAsTable()` para evitar dependencias de storage
- **Arquitectura Medallion**: Bronze → Silver → Gold
- **Inline Comments**: Todo el código con explicaciones en español (qué/por qué/cómo)
- **Sin Azure Storage Account**: Labs completamente autocontenidos

## Requisitos Previos

- Workspace de Azure Databricks (Premium tier recomendado)
- Acceso para crear clusters (Standard_DS3_v2 o superior)
- Conocimientos básicos de Python y SQL

## Cómo Usar Este Módulo

1. **Revisar documentación** en orden secuencial (01 → 06)
2. **Ejecutar laboratorios** en Databricks workspace
3. **Importar notebooks** a `/Shared/databricks-intro/`
4. **Seguir demos** para ver flujos completos

## Tiempo Total Estimado

- **Documentación**: ~6-8 horas de lectura
- **Laboratorios**: ~4-5 horas de práctica
- **Total**: ~10-13 horas (6 sesiones de 2 horas)

## Recursos Adicionales

Ver carpeta `../resources/` para:
- Checklist de KT
- FAQ
- Glosario de términos
- Referencias externas
