# Azure Databricks: Knowledge Transfer - Sesión 1

> 📚 Repositorio de capacitación para Azure Databricks - Introducción y conceptos fundamentales

## 🎯 Objetivo del Training

Introducir Azure Databricks y demostrar cómo procesa y transforma los datos que alimentan soluciones empresariales, proporcionando una base sólida para la operación y mantenimiento de la plataforma.

## 📋 Contenido del Repositorio

### � Notebooks Jupyter (NUEVO)
- [Lab 01 - Workspace](./notebooks/lab-01-workspace.ipynb) - DataFrames, transformaciones, Delta Lake
- [Lab 02 - Clusters](./notebooks/lab-02-clusters.ipynb) - Autoscaling, calculadora de costos
- [Lab 03 - Notebooks Avanzados](./notebooks/lab-03-notebooks.ipynb) - Magic commands, widgets
- [Lab 04 - Transformación](./notebooks/lab-04-transformacion.ipynb) - Arquitectura Medallion
- [Lab 05 - Jobs](./notebooks/lab-05-jobs.ipynb) - Workflows multi-task
- [Lab 06 - Integración](./notebooks/lab-06-integracion.ipynb) - CSV/JSON/Parquet, streaming

**📘 [Ver Guía de Notebooks](./notebooks/README.md)** | **📖 [Documentación Completa del Código](./CODIGO_DOCUMENTACION.md)**

### 📖 Documentación de Conceptos
- [01 - Introducción a Azure Databricks](./docs/01-introduccion-databricks.md)
- [02 - Componentes Clave](./docs/02-componentes-clave.md)
- [03 - Arquitectura e Integración](./docs/03-arquitectura-integracion.md)
- [04 - Clusters y Gestión de Costos](./docs/04-clusters-costos.md)
- [05 - Notebooks y Jobs](./docs/05-notebooks-jobs.md)
- [06 - Integración con Servicios Azure](./docs/06-integracion-azure.md)

### 🔬 Laboratorios y Ejercicios (Markdown)
- [Lab 01 - Configuración Inicial y Workspace](./labs/lab-01-configuracion-inicial.md)
- [Lab 02 - Creación y Configuración de Clusters](./labs/lab-02-clusters.md)
- [Lab 03 - Trabajando con Notebooks](./labs/lab-03-notebooks.md)
- [Lab 04 - Transformación de Datos](./labs/lab-04-transformacion-datos.md)
- [Lab 05 - Configuración y Ejecución de Jobs](./labs/lab-05-jobs.md)
- [Lab 06 - Integración de Datos](./labs/lab-06-integracion-datos.md)

### 🎬 Demos
- [Demo 01 - Recorrido por Notebook de Transformación](./demos/demo-01-notebook-transformacion.md)
- [Demo 02 - Ejecución de Job y Revisión de Resultados](./demos/demo-02-job-ejecucion.md)

### 📊 Recursos Adicionales
- [Checklist de KT con Partner](./resources/checklist-kt.md)
- [Preguntas Frecuentes](./resources/faq.md)
- [Glosario de Términos](./resources/glosario.md)
- [Referencias y Enlaces Útiles](./resources/referencias.md)

## 🗓️ Agenda de la Sesión 1

| Tiempo | Tema | Materiales |
|--------|------|-----------|
| 20 min | Introducción a Azure Databricks | [docs/01](./docs/01-introduccion-databricks.md) |
| 20 min | Componentes clave: workspaces, clusters, notebooks, jobs | [docs/02](./docs/02-componentes-clave.md) |
| 20 min | Cómo encaja Databricks en la arquitectura | [docs/03](./docs/03-arquitectura-integracion.md) |
| 30 min | Demo 1: recorrido por notebook de transformación | [demos/demo-01](./demos/demo-01-notebook-transformacion.md) |
| 20 min | Demo 2: ejecución de job y revisión de resultados | [demos/demo-02](./demos/demo-02-job-ejecucion.md) |
| 10 min | Q&A y checklist para el KT | [resources/checklist-kt](./resources/checklist-kt.md) |

## 🎓 Qué se Llevarán del KT

Al finalizar esta sesión, los participantes comprenderán:

✅ **Qué es Databricks** y cuál es su rol en la arquitectura de la solución  
✅ **Qué notebooks/jobs** forman parte de la solución y cómo operarlos  
✅ **Flujo de datos**: De dónde vienen los datos y a dónde se escriben (Cosmos DB, Storage, AI Search)  
✅ **Conceptos básicos** de clusters, tipos y gestión de costos  
✅ **Qué preguntar** al partner sobre la operación de los jobs en producción  

## 🚀 Cómo Usar Este Repositorio

### 📖 Para Lectura y Teoría
1. **Documentación Conceptual** (`/docs/`): Fundamentos, arquitectura, mejores prácticas
2. **Guías de Laboratorio** (`/labs/`): Instrucciones paso a paso en Markdown

### 💻 Para Práctica Hands-On
1. **Notebooks Jupyter** (`/notebooks/`): Código ejecutable listo para usar
   - Sube los `.ipynb` a tu Databricks Workspace
   - O clona el repo usando Databricks Repos
   - Ejecuta celda por celda siguiendo las instrucciones

2. **Documentación del Código** (`CODIGO_DOCUMENTACION.md`):
   - Explicaciones línea por línea de todo el código
   - Docstrings, comentarios detallados, ejemplos
   - Conceptos de PySpark, Delta Lake, optimizaciones
   - **¡Consultar mientras ejecutas los notebooks!**

### 👨‍🏫 Para Instructores
- Seguir el orden de la documentación
- Utilizar las demos como guía
- Referir a los notebooks para ejercicios prácticos

### 👨‍🎓 Para Participantes
- Revisar documentación antes de la sesión
- Completar laboratorios en orden (Lab 01 → Lab 06)
- Ejecutar notebooks en Databricks Workspace
- Consultar `CODIGO_DOCUMENTACION.md` para entender el código en profundidad

### ⚙️ Para Operadores
- Consultar el checklist y preguntas para el partner
- Revisar demos de jobs para operación en producción

## 📚 Prerequisitos

- Acceso a un workspace de Azure Databricks
- Cuenta de Azure con permisos de Contributor
- Conocimientos básicos de:
  - Python o Scala
  - SQL
  - Conceptos de Big Data

## 🤝 Contribuciones

Este repositorio es material de capacitación interno. Para sugerencias o mejoras, contactar al equipo de Knowledge Transfer.

## 📄 Licencia

Uso interno - Material de capacitación empresarial

---

**Última actualización**: Mayo 2026  
**Versión**: 1.0  
**Contacto**: Equipo de Knowledge Transfer
