# 📓 Notebooks - Azure Databricks Training

Esta carpeta contiene notebooks Jupyter (.ipynb) para los ejercicios prácticos del curso de Azure Databricks.

## 📦 Contenido

| Notebook | Descripción | Duración | Conceptos Clave |
|----------|-------------|----------|-----------------|
| **lab-01-workspace.ipynb** | Configuración inicial y operaciones básicas | 30 min | DataFrames, transformaciones, Delta Lake, SQL |
| **lab-02-clusters.ipynb** | Autoscaling y optimización de costos | 30-40 min | Autoscaling, DBU pricing, calculadora de costos |
| **lab-03-notebooks.ipynb** | Funciones avanzadas de notebooks | 35-45 min | Magic commands, widgets, debugging |
| **lab-04-transformacion.ipynb** | Arquitectura Medallion completa | 45-50 min | Bronze/Silver/Gold, deduplicación, Delta ops |
| **lab-05-jobs.ipynb** | Workflows multi-task | 35-45 min | Parametrización, orquestación, error handling |
| **lab-06-integracion.ipynb** | Integración de datos y streaming | 40-45 min | CSV/JSON/Parquet, joins, Auto Loader |

---

## 🚀 Cómo Usar los Notebooks

### Opción 1: Subir a Databricks Workspace

1. **Accede a tu Databricks Workspace**
2. **Navega a Workspace → Users → [tu usuario]**
3. **Click derecho → Import**
4. **Selecciona los archivos .ipynb**
5. **Abre el notebook y selecciona un cluster**

### Opción 2: Clonar desde Git

```bash
# En Databricks Workspace → Repos
# Click "Add Repo" → Pega la URL del repositorio
https://github.com/alemcuevas/databricks-intro.git
```

Los notebooks estarán disponibles en la carpeta `notebooks/`.

---

## 📖 Documentación del Código

Para entender en profundidad cada línea de código, consulta:

### 📘 [CODIGO_DOCUMENTACION.md](../CODIGO_DOCUMENTACION.md)

Este archivo contiene:
- ✅ Explicación detallada de cada concepto
- ✅ Comentarios línea por línea del código
- ✅ Diagramas y ejemplos adicionales
- ✅ Best practices y anti-patterns
- ✅ Opciones avanzadas de configuración
- ✅ Troubleshooting común

**Estructura de la documentación:**
- **Imports y configuración**: Qué hace cada biblioteca
- **Funciones**: Docstrings detallados con parámetros y retornos
- **Transformaciones**: Explicación de cada operación de Spark
- **Patrones**: Medallion, deduplicación, data quality
- **Performance**: Optimizaciones y mejores prácticas

---

## 🔧 Pre-requisitos

### Cluster Configuration (Recomendado)

```yaml
Databricks Runtime: 13.3 LTS (o superior)
Worker Type: Standard_DS3_v2 (4 cores, 14GB RAM)
Workers: 2-4 (con autoscaling habilitado)
Driver Type: Same as worker
Autoscaling: Habilitado (2 min, 10 max)
Auto Termination: 30 minutos
```

### Librerías Requeridas

Las siguientes librerías vienen pre-instaladas en Databricks Runtime:
- **PySpark 3.4+**
- **Delta Lake 2.4+**
- **Python 3.10+**

No se requieren instalaciones adicionales para los labs básicos.

---

## 📝 Orden de Ejecución Recomendado

### Para Principiantes:
1. **Lab 01** → Fundamentos
2. **Lab 02** → Clusters y costos
3. **Lab 03** → Notebooks avanzados
4. **Lab 04** → Transformaciones
5. **Lab 05** → Jobs
6. **Lab 06** → Integración

### Para Usuarios con Experiencia en Spark:
- Puedes empezar desde **Lab 04** (Medallion Architecture)
- O **Lab 06** (Streaming con Auto Loader)

---

## 🎯 Objetivos de Aprendizaje

Al completar todos los notebooks, serás capaz de:

- ✅ Crear y manipular DataFrames de PySpark
- ✅ Aplicar transformaciones, filtros y agregaciones
- ✅ Diseñar e implementar arquitectura Medallion (Bronze/Silver/Gold)
- ✅ Realizar operaciones Delta Lake (MERGE, OPTIMIZE, Time Travel)
- ✅ Parametrizar notebooks para Jobs programados
- ✅ Integrar datos de múltiples formatos (CSV, JSON, Parquet)
- ✅ Implementar streaming con Auto Loader
- ✅ Optimizar costos y performance de clusters
- ✅ Aplicar data quality checks y error handling
- ✅ Orquestar workflows multi-task

---

## 💾 Almacenamiento de Datos

Todos los notebooks utilizan **almacenamiento local en DBFS** (`/tmp/lab0X/`) para:
- ✅ No requiere configuración de Azure Storage Account
- ✅ Ideal para aprendizaje y pruebas
- ✅ Datos se eliminan automáticamente al terminar la sesión

### Para Producción:

Reemplaza `/tmp/` con rutas de **Azure Data Lake Storage (ADLS Gen2)**:

```python
# Desarrollo (labs)
path = "/tmp/lab01/empleados"

# Producción
path = "abfss://container@storageaccount.dfs.core.windows.net/data/empleados"
```

---

## 🐛 Troubleshooting

### Error: "Cluster no disponible"
**Solución:** Inicia un cluster desde la UI de Databricks antes de ejecutar notebooks.

### Error: "AnalysisException: Path does not exist"
**Solución:** Verifica que ejecutaste las celdas previas que crean los datos.

### Error: "java.lang.OutOfMemoryError"
**Solución:** Reduce el `batch_size` o incrementa el tamaño del cluster.

### Performance lento
**Solución:** 
- Habilita autoscaling
- Usa cache para DataFrames reutilizados
- Reduce shuffles innecesarios

### Delta Lake: "ConcurrentAppendException"
**Solución:** Dos procesos intentan escribir simultáneamente. Usa MERGE en lugar de writes concurrentes.

---

## 📊 Monitoring y Debugging

### Spark UI

Accede para ver:
- **Jobs**: Operaciones de alto nivel
- **Stages**: Pasos de cada job
- **Storage**: DataFrames cacheados
- **SQL**: Query plans y performance

**Ubicación:** Cluster → Spark UI tab

### Logs

```python
# Ver logs del driver
%fs head /databricks/driver/logs/stderr

# Ver logs de workers
%fs ls /databricks/spark/work/
```

---

## 🔐 Seguridad y Best Practices

### Credenciales

❌ **NUNCA** hardcodear credenciales:
```python
password = "mypassword123"  # ❌ MALO
```

✅ **SIEMPRE** usar Azure Key Vault:
```python
password = dbutils.secrets.get(scope="my-scope", key="password")  # ✅ BUENO
```

### Datos Sensibles

- Usar encryption at rest (Delta Lake lo soporta)
- Aplicar data masking para PII
- Implementar row-level security si es necesario

---

## 📚 Recursos Adicionales

### Documentación Oficial
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Docs](https://docs.databricks.com/)
- [Delta Lake](https://docs.delta.io/)

### Tutoriales
- [Databricks Academy](https://academy.databricks.com/)
- [Spark By Examples](https://sparkbyexamples.com/)

### Comunidad
- [Databricks Community](https://community.databricks.com/)
- [Stack Overflow - pyspark tag](https://stackoverflow.com/questions/tagged/pyspark)

---

## 🤝 Contribuciones

¿Encontraste un error o tienes una mejora?

1. Fork el repositorio
2. Crea una rama para tu cambio
3. Haz commit de tus cambios
4. Abre un Pull Request

---

## 📄 Licencia

Este material es de uso educativo. Consulta el archivo LICENSE en la raíz del repositorio.

---

## ✨ Créditos

Desarrollado para el training de Azure Databricks.

**Última actualización:** Mayo 2026

---

¿Listo para empezar? Abre **lab-01-workspace.ipynb** y comienza tu viaje con Databricks! 🚀
