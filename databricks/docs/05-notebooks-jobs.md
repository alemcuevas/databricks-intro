# Notebooks y Jobs - Operaciones y Mejores Prácticas

## 📚 Índice

1. [Notebooks en Producción](#notebooks-en-producción)
2. [Desarrollo de Notebooks](#desarrollo-de-notebooks)
3. [Jobs: Configuración y Operación](#jobs-configuración-y-operación)
4. [Monitoreo y Troubleshooting](#monitoreo-y-troubleshooting)
5. [Mejores Prácticas](#mejores-prácticas)

---

## 1. Notebooks en Producción

### 1.1 Estructura Recomendada de un Notebook

Un notebook productivo debe seguir esta estructura:

```
┌─────────────────────────────────────────┐
│ 1. DOCUMENTACIÓN Y METADATA             │
│    - Propósito, autor, versión          │
│    - Historial de cambios               │
├─────────────────────────────────────────┤
│ 2. CONFIGURACIÓN E IMPORTS              │
│    - Librerías                          │
│    - Parámetros y widgets               │
│    - Secrets y credenciales             │
├─────────────────────────────────────────┤
│ 3. FUNCIONES Y UTILIDADES               │
│    - Definiciones reutilizables         │
│    - Data quality checks                │
├─────────────────────────────────────────┤
│ 4. LÓGICA PRINCIPAL                     │
│    - Ingesta de datos                   │
│    - Transformaciones                   │
│    - Validaciones                       │
├─────────────────────────────────────────┤
│ 5. ESCRITURA Y EXPORT                   │
│    - Guardar resultados                 │
│    - Actualizar tablas                  │
├─────────────────────────────────────────┤
│ 6. LOGGING Y NOTIFICACIONES             │
│    - Métricas de ejecución              │
│    - Resumen y alertas                  │
└─────────────────────────────────────────┘
```

### 1.2 Ejemplo de Template Productivo

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Nombre del Notebook: ETL de Datos de Ventas
# MAGIC 
# MAGIC ## 📋 Metadata
# MAGIC | Campo | Valor |
# MAGIC |-------|-------|
# MAGIC | **Autor** | Data Engineering Team |
# MAGIC | **Versión** | 2.1.0 |
# MAGIC | **Última actualización** | 2026-05-22 |
# MAGIC | **Propósito** | Procesar datos de ventas diarios |
# MAGIC | **Frecuencia** | Diario 3:00 AM |
# MAGIC | **SLA** | 30 minutos |
# MAGIC 
# MAGIC ## 📝 Changelog
# MAGIC - v2.1.0 (2026-05-22): Agregada validación de duplicados
# MAGIC - v2.0.0 (2026-05-15): Migración a Delta Lake
# MAGIC - v1.0.0 (2026-04-01): Versión inicial

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Configuración e Imports

# COMMAND ----------

# Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
import json

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Información de ejecución
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()

logger.info(f"Ejecutando notebook: {notebook_path}")
logger.info(f"Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parámetros

# COMMAND ----------

# Crear widgets para parámetros
dbutils.widgets.text("fecha_proceso", "2026-05-22", "Fecha de Proceso (YYYY-MM-DD)")
dbutils.widgets.dropdown("environment", "prod", ["dev", "test", "prod"], "Environment")
dbutils.widgets.dropdown("mode", "incremental", ["full", "incremental"], "Processing Mode")

# Leer parámetros
fecha_proceso = dbutils.widgets.get("fecha_proceso")
environment = dbutils.widgets.get("environment")
mode = dbutils.widgets.get("mode")

# Validar fecha
try:
    fecha_dt = datetime.strptime(fecha_proceso, "%Y-%m-%d")
except ValueError:
    raise ValueError(f"Fecha inválida: {fecha_proceso}. Formato esperado: YYYY-MM-DD")

logger.info(f"Parámetros: fecha={fecha_proceso}, env={environment}, mode={mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secrets y Credenciales

# COMMAND ----------

# Obtener secrets desde Key Vault
try:
    storage_key = dbutils.secrets.get("keyvault", "storage-account-key")
    cosmos_key = dbutils.secrets.get("keyvault", "cosmos-db-key")
    logger.info("✅ Secrets cargados exitosamente")
except Exception as e:
    logger.error(f"❌ Error al cargar secrets: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Funciones Auxiliares

# COMMAND ----------

def validate_dataframe(df, expected_columns, min_rows=1):
    """
    Valida que un DataFrame tenga columnas esperadas y cantidad mínima de registros.
    
    Args:
        df: DataFrame a validar
        expected_columns: Lista de columnas esperadas
        min_rows: Mínimo de registros esperados
    
    Raises:
        ValueError: Si la validación falla
    """
    # Validar columnas
    missing_cols = set(expected_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Columnas faltantes: {missing_cols}")
    
    # Validar cantidad de registros
    count = df.count()
    if count < min_rows:
        raise ValueError(f"Registros insuficientes: {count} < {min_rows}")
    
    logger.info(f"✅ Validación exitosa: {count:,} registros, columnas OK")
    return True

def log_metrics(stage, metrics_dict):
    """
    Log métricas estructuradas.
    
    Args:
        stage: Nombre del stage (ej: "ingestion", "transformation")
        metrics_dict: Diccionario con métricas
    """
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "notebook": notebook_path,
        "run_id": run_id,
        "stage": stage,
        **metrics_dict
    }
    logger.info(f"METRICS: {json.dumps(metrics)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Lógica Principal

# COMMAND ----------

try:
    start_time = datetime.now()
    
    # 1. Ingesta
    logger.info("Iniciando ingesta...")
    df_raw = spark.read \
        .format("parquet") \
        .load(f"/mnt/bronze/sales/{fecha_proceso}/")
    
    count_ingested = df_raw.count()
    log_metrics("ingestion", {
        "records": count_ingested,
        "source": "bronze"
    })
    
    # Validar
    validate_dataframe(df_raw, ["transaction_id", "amount", "date"], min_rows=1)
    
    # 2. Transformación
    logger.info("Aplicando transformaciones...")
    df_transformed = df_raw \
        .filter(col("amount") > 0) \
        .withColumn("processed_at", lit(current_timestamp()))
    
    count_transformed = df_transformed.count()
    filtered_out = count_ingested - count_transformed
    
    log_metrics("transformation", {
        "records_in": count_ingested,
        "records_out": count_transformed,
        "filtered": filtered_out
    })
    
    # 3. Escribir
    logger.info("Escribiendo resultados...")
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"/mnt/silver/sales/{fecha_proceso}/")
    
    # Resumen final
    duration = (datetime.now() - start_time).total_seconds()
    
    summary = {
        "status": "SUCCESS",
        "fecha_proceso": fecha_proceso,
        "duration_seconds": duration,
        "records_processed": count_transformed
    }
    
    log_metrics("completion", summary)
    logger.info(f"✅ Notebook completado exitosamente en {duration:.1f}s")
    
    # Retornar resultado (para orquestación)
    dbutils.notebook.exit(json.dumps(summary))
    
except Exception as e:
    logger.error(f"❌ Error en notebook: {str(e)}", exc_info=True)
    
    # Resumen de error
    error_summary = {
        "status": "FAILED",
        "fecha_proceso": fecha_proceso,
        "error": str(e)
    }
    
    log_metrics("error", error_summary)
    
    # Re-raise para que el job falle
    raise
```

---

## 2. Desarrollo de Notebooks

### 2.1 Uso de Magic Commands

| Magic | Propósito | Ejemplo |
|-------|-----------|---------|
| `%md` | Markdown para documentación | `%md # Título` |
| `%python` | Cambiar a Python | `%python print("Hello")` |
| `%sql` | Ejecutar SQL | `%sql SELECT * FROM table` |
| `%scala` | Cambiar a Scala | `%scala val x = 1` |
| `%r` | Cambiar a R | `%r summary(df)` |
| `%sh` | Ejecutar shell command | `%sh ls -la` |
| `%run` | Ejecutar otro notebook | `%run ./Utils/common` |
| `%fs` | File system commands | `%fs ls /mnt/data` |

**Ejemplo de uso de %run**:

```python
# Notebook: Utils/CommonFunctions
# COMMAND ----------

def clean_string(s):
    return s.strip().upper()

def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except:
        return False

# COMMAND ----------

# Notebook principal
# COMMAND ----------

%run ./Utils/CommonFunctions

# Ahora puedes usar las funciones
result = clean_string("  hello world  ")  # "HELLO WORLD"
```

### 2.2 Widgets para Parametrización

**Tipos de widgets**:

```python
# Text widget
dbutils.widgets.text("param1", "default_value", "Label")

# Dropdown
dbutils.widgets.dropdown("env", "prod", ["dev", "test", "prod"], "Environment")

# Combobox (dropdown con input libre)
dbutils.widgets.combobox("region", "us-east", ["us-east", "us-west", "eu"], "Region")

# Multiselect
dbutils.widgets.multiselect("categories", "A", ["A", "B", "C"], "Categories")

# Leer valor
value = dbutils.widgets.get("param1")

# Remover widget
dbutils.widgets.remove("param1")

# Remover todos
dbutils.widgets.removeAll()
```

### 2.3 Comunicación entre Notebooks

**Llamar a otro notebook y recibir resultado**:

```python
# Notebook A (caller)
result = dbutils.notebook.run(
    "/Users/me/ProcessData",
    timeout_seconds=3600,
    arguments={
        "fecha": "2026-05-22",
        "mode": "incremental"
    }
)

# result contiene lo que Notebook B retornó con dbutils.notebook.exit()
result_dict = json.loads(result)
print(f"Procesados: {result_dict['records_processed']} registros")

# Notebook B (callee)
# ... procesamiento ...
summary = {"records_processed": 1000, "status": "SUCCESS"}
dbutils.notebook.exit(json.dumps(summary))
```

---

## 3. Jobs: Configuración y Operación

### 3.1 Anatomía de un Job

```
Job: ETL_Pipeline_Daily
│
├── Configuración General
│   ├── Schedule: 0 0 3 * * ? (diario 3 AM)
│   ├── Timezone: America/New_York
│   ├── Max concurrent runs: 1
│   └── Timeout: 2 horas
│
├── Tasks
│   ├── Task 1: ingest_data
│   │   ├── Type: Notebook
│   │   ├── Path: /Production/ETL/ingest
│   │   ├── Cluster: job-cluster-small
│   │   ├── Dependencies: None
│   │   ├── Retry: 2 veces
│   │   └── Timeout: 30 min
│   │
│   ├── Task 2: transform_data
│   │   ├── Depends on: ingest_data
│   │   ├── Cluster: job-cluster-medium
│   │   └── ... (similar config)
│   │
│   └── Task 3: export_results
│       ├── Depends on: transform_data
│       └── ...
│
├── Clusters
│   ├── job-cluster-small: 2-4 workers, DS3_v2
│   └── job-cluster-medium: 4-8 workers, DS4_v2
│
└── Notifications
    ├── On Success: data-team@empresa.com
    ├── On Failure: oncall@empresa.com, PagerDuty
    └── On Start: (none)
```

### 3.2 Configuración de Cluster para Jobs

**Configuración óptima para Job Cluster**:

```json
{
  "cluster_name": "job-cluster-etl",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  },
  "azure_attributes": {
    "availability": "SPOT_WITH_FALLBACK_AZURE",  // Usa Spot con fallback
    "spot_bid_max_price": -1  // Sin límite de precio
  },
  "enable_elastic_disk": true,
  "init_scripts": [
    {
      "dbfs": {
        "destination": "dbfs:/scripts/install_custom_libs.sh"
      }
    }
  ],
  "custom_tags": {
    "Project": "ETL",
    "Environment": "Production",
    "CostCenter": "DataEngineering"
  }
}
```

### 3.3 Strategies de Retry

**Configuración de retry en Task**:

```json
{
  "max_retries": 2,
  "min_retry_interval_millis": 300000,  // 5 minutos
  "retry_on_timeout": true
}
```

**Retry condicional en código**:

```python
from functools import wraps
import time

def retry_on_failure(max_attempts=3, delay=60):
    """Decorator para retry automático"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Falló después de {max_attempts} intentos")
                        raise
                    logger.warning(f"Intento {attempt + 1} falló: {e}. Reintentando en {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry_on_failure(max_attempts=3, delay=30)
def write_to_cosmos(df):
    df.write.format("cosmos.oltp").options(...).save()
```

### 3.4 Orquestación Compleja

**Job con branching (parallel execution)**:

```
         ┌─────────────┐
         │   Ingest    │
         └──────┬──────┘
                │
        ┌───────┴───────┐
        │               │
┌───────▼──────┐ ┌─────▼────────┐
│ Transform_A  │ │ Transform_B  │  (En paralelo)
└───────┬──────┘ └─────┬────────┘
        │               │
        └───────┬───────┘
                │
         ┌──────▼──────┐
         │  Aggregate  │
         └──────┬──────┘
                │
         ┌──────▼──────┐
         │   Export    │
         └─────────────┘
```

**Configuración JSON**:

```json
{
  "name": "ETL_Pipeline_Parallel",
  "tasks": [
    {
      "task_key": "ingest",
      "notebook_task": {"notebook_path": "/ETL/ingest"}
    },
    {
      "task_key": "transform_a",
      "depends_on": [{"task_key": "ingest"}],
      "notebook_task": {"notebook_path": "/ETL/transform_a"}
    },
    {
      "task_key": "transform_b",
      "depends_on": [{"task_key": "ingest"}],
      "notebook_task": {"notebook_path": "/ETL/transform_b"}
    },
    {
      "task_key": "aggregate",
      "depends_on": [
        {"task_key": "transform_a"},
        {"task_key": "transform_b"}
      ],
      "notebook_task": {"notebook_path": "/ETL/aggregate"}
    },
    {
      "task_key": "export",
      "depends_on": [{"task_key": "aggregate"}],
      "notebook_task": {"notebook_path": "/ETL/export"}
    }
  ]
}
```

---

## 4. Monitoreo y Troubleshooting

### 4.1 Métricas Clave

**En la UI del Job**:

| Métrica | Qué Observar | Acción si Anormal |
|---------|--------------|-------------------|
| **Duration** | Tiempo total de ejecución | Si > baseline + 50%, investigar |
| **Success Rate** | % de ejecuciones exitosas | Si < 95%, investigar causas |
| **Cost** | Costo por ejecución | Si > presupuesto, optimizar |
| **Task Duration** | Tiempo por task | Identificar tasks lentas |

**Query para analizar historial**:

```sql
-- En Databricks SQL o notebook
SELECT
    run_id,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes,
    state,
    result_state
FROM system.workflows.job_runs
WHERE job_id = <tu_job_id>
    AND start_time >= current_date() - INTERVAL 30 DAYS
ORDER BY start_time DESC
```

### 4.2 Alertas Avanzadas

**Configurar alertas en Job**:

```json
{
  "email_notifications": {
    "on_success": ["data-team@empresa.com"],
    "on_failure": ["oncall@empresa.com"],
    "on_duration_warning_threshold_exceeded": ["manager@empresa.com"]
  },
  "webhook_notifications": {
    "on_failure": [
      {
        "id": "pagerduty-webhook",
        "url": "https://events.pagerduty.com/integration/..."
      }
    ]
  },
  "health": {
    "rules": [
      {
        "metric": "RUN_DURATION_SECONDS",
        "op": "GREATER_THAN",
        "value": 3600  // Alertar si > 1 hora
      }
    ]
  }
}
```

### 4.3 Debugging de Fallos

**Checklist de debugging**:

1. **Ver el error en Job UI**
   - Job → Latest Run → Task fallida → Ver output

2. **Revisar logs del cluster**
   - Task → Cluster Logs → driver.log, executor.log

3. **Ver Spark UI**
   - Task → Spark UI → Identificar stage lento/fallido

4. **Reproducir localmente**
   - Clonar el notebook
   - Usar mismo cluster type
   - Ejecutar con mismo input

5. **Revisar cambios recientes**
   - Git history del notebook
   - Cambios en datos de entrada
   - Cambios en dependencias

**Errores comunes y soluciones**:

| Error | Causa | Solución |
|-------|-------|----------|
| `OutOfMemoryError` | DataFrame muy grande en driver | Evitar collect(), usar display() |
| `FileNotFoundException` | Path incorrecto | Verificar path, permisos |
| `AnalysisException: Column not found` | Esquema cambió | Revisar upstream, validar schema |
| `Timeout` | Query muy lento | Optimizar query, aumentar timeout |

---

## 5. Mejores Prácticas

### 5.1 Desarrollo de Notebooks

**✅ DO**:
- ✅ Documentar propósito y metadata
- ✅ Usar widgets para parametrización
- ✅ Validar inputs y outputs
- ✅ Implementar logging estructurado
- ✅ Manejar errores explícitamente
- ✅ Retornar resumen con dbutils.notebook.exit()
- ✅ Mantener notebooks < 100 celdas
- ✅ Separar funciones en notebooks de utilidades

**❌ DON'T**:
- ❌ Hardcodear valores (usar widgets)
- ❌ Hardcodear secrets (usar Key Vault)
- ❌ Usar `collect()` en DataFrames grandes
- ❌ Ignorar errores con try/except vacío
- ❌ Mezclar lógica de negocio con configuración
- ❌ Crear notebooks de 500+ líneas

### 5.2 Configuración de Jobs

**✅ DO**:
- ✅ Usar Job Clusters (no All-Purpose)
- ✅ Habilitar autoscaling
- ✅ Configurar retries (2-3)
- ✅ Establecer timeouts razonables
- ✅ Usar spot instances para workers
- ✅ Configurar alertas para success y failure
- ✅ Usar tags para cost tracking

**❌ DON'T**:
- ❌ Usar All-Purpose cluster para jobs
- ❌ Permitir concurrent runs sin control
- ❌ Configurar timeout muy bajo
- ❌ Ignorar alertas de duración
- ❌ Crear jobs sin documentación

### 5.3 Gestión de Código

**Control de versiones (Git)**:

```bash
# Estructura recomendada en repo
/databricks-project/
├── notebooks/
│   ├── production/
│   │   ├── etl/
│   │   │   ├── ingest.py
│   │   │   ├── transform.py
│   │   │   └── export.py
│   │   └── analytics/
│   ├── development/
│   └── utils/
│       └── common_functions.py
├── jobs/
│   └── job_configs.json
├── tests/
│   └── test_transformations.py
└── README.md
```

**CI/CD para Notebooks**:

```yaml
# Azure DevOps Pipeline (azure-pipelines.yml)
trigger:
  branches:
    include:
      - main
      - release/*

stages:
  - stage: Test
    jobs:
      - job: UnitTests
        steps:
          - task: UsePythonVersion@0
          - script: |
              pip install pytest pyspark
              pytest tests/
  
  - stage: Deploy
    dependsOn: Test
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: DeployNotebooks
        steps:
          - task: UsePythonVersion@0
          - script: |
              pip install databricks-cli
              databricks workspace import_dir notebooks /Production --overwrite
```

### 5.4 Testing

**Unit tests para funciones**:

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from notebooks.utils.common_functions import clean_string, validate_dataframe

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()

def test_clean_string():
    assert clean_string("  hello  ") == "HELLO"
    assert clean_string("") == ""

def test_validate_dataframe(spark):
    # Create test DataFrame
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    
    # Should pass
    assert validate_dataframe(df, ["id", "value"], min_rows=1) == True
    
    # Should fail
    with pytest.raises(ValueError):
        validate_dataframe(df, ["id", "missing_col"], min_rows=1)
```

---

## 📚 Recursos Adicionales

- [Notebook Best Practices](https://docs.databricks.com/notebooks/best-practices.html)
- [Jobs Documentation](https://docs.databricks.com/jobs/)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)

---

**Siguiente**: [Integración con Servicios Azure](./06-integracion-azure.md)
