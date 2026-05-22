# 02 - Componentes Clave de Azure Databricks

## 📝 Índice
- [Workspaces](#workspaces)
- [Clusters](#clusters)
- [Notebooks](#notebooks)
- [Jobs](#jobs)
- [Bibliotecas y Dependencias](#bibliotecas-y-dependencias)
- [DBFS - Databricks File System](#dbfs---databricks-file-system)

---

## Workspaces

### ¿Qué es un Workspace?

Un **Workspace** es el entorno principal de trabajo en Azure Databricks. Es un espacio colaborativo donde los equipos pueden:

- 📝 Crear y compartir notebooks
- 🖥️ Gestionar clusters
- ⚙️ Configurar jobs
- 📊 Organizar datos y resultados
- 👥 Colaborar en tiempo real

### Estructura de un Workspace

```
Mi-Workspace/
├── 📁 Workspace
│   ├── 📁 Users
│   │   ├── 📁 user1@empresa.com
│   │   │   ├── 📓 ETL_Ventas.py
│   │   │   └── 📓 Analisis_Clientes.sql
│   │   └── 📁 user2@empresa.com
│   ├── 📁 Shared
│   │   ├── 📓 Transformaciones_Comunes
│   │   └── 📓 Utilidades
│   └── 📁 Repos (Git integration)
│       └── 📁 mi-proyecto-databricks
├── 💻 Compute (Clusters)
│   ├── 🟢 cluster-prod-01 (Running)
│   ├── 🔴 cluster-dev-01 (Terminated)
│   └── 🟡 cluster-ml-02 (Starting)
├── 📊 Data
│   ├── Databases
│   ├── Tables
│   └── File System (DBFS)
├── ⚙️ Workflows (Jobs)
│   ├── job-etl-daily
│   ├── job-ml-training
│   └── job-reporting
└── 🔒 Settings
    ├── User Management
    ├── Cluster Policies
    └── Access Control
```

### Organización de Workspaces

```python
# Ejemplo de estructura recomendada
"""
Workspace/
├── 01_Ingestion/
│   ├── ingest_cosmos_data.py
│   ├── ingest_blob_storage.py
│   └── validate_sources.py
│
├── 02_Transform/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── data_quality_checks.py
│
├── 03_Analysis/
│   ├── customer_segmentation.py
│   ├── sales_analysis.sql
│   └── ml_predictions.py
│
├── 04_Export/
│   ├── export_to_cosmos.py
│   ├── export_to_search.py
│   └── export_to_synapse.py
│
└── Utils/
    ├── common_functions.py
    ├── config.py
    └── logger.py
"""
```

### Permisos en Workspace

| Nivel | Permisos | Uso Típico |
|-------|----------|------------|
| **Can View** | Solo lectura | Stakeholders, auditores |
| **Can Run** | Ejecutar notebooks/jobs | Operadores |
| **Can Edit** | Modificar contenido | Desarrolladores |
| **Can Manage** | Control total | Administradores |

---

## Clusters

### ¿Qué es un Cluster?

Un **Cluster** es un conjunto de máquinas virtuales (VMs) que proporcionan la capacidad de cómputo para ejecutar cargas de trabajo de Databricks.

### Anatomía de un Cluster

```
┌─────────────────────────────────────────────┐
│              CLUSTER                        │
├─────────────────────────────────────────────┤
│                                             │
│  ┌──────────────────────┐                  │
│  │    DRIVER NODE       │                  │
│  │  ┌────────────────┐  │                  │
│  │  │ SparkContext   │  │                  │
│  │  │ Coordinator    │  │                  │
│  │  │ Scheduler      │  │                  │
│  │  └────────────────┘  │                  │
│  └──────────┬───────────┘                  │
│             │                               │
│    ┌────────▼────────┐                     │
│    │  WORKER NODES   │                     │
│    │  ┌───┐ ┌───┐   │                     │
│    │  │ 1 │ │ 2 │   │  Executors          │
│    │  └───┘ └───┘   │  (Procesar tareas)  │
│    │  ┌───┐ ┌───┐   │                     │
│    │  │ 3 │ │ 4 │   │                     │
│    │  └───┘ └───┘   │                     │
│    └─────────────────┘                     │
│                                             │
└─────────────────────────────────────────────┘
```

### Tipos de Clusters

#### 1. **All-Purpose Clusters (Interactivos)**

- 💡 Para desarrollo y análisis interactivo
- 🔄 Se pueden compartir entre múltiples usuarios
- ⏱️ Permanecen activos hasta que se terminan manualmente o por timeout
- 💰 Más costosos (precio completo DBU)

```python
# Configuración típica de All-Purpose Cluster
{
    "cluster_name": "dev-interactive-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "autoscale": {
        "min_workers": 2,
        "max_workers": 8
    },
    "autotermination_minutes": 120,  # Se apaga después de 2 horas sin uso
    "cluster_log_conf": {
        "dbfs": {
            "destination": "dbfs:/cluster-logs"
        }
    }
}
```

#### 2. **Job Clusters (Automatizados)**

- ⚙️ Para ejecución de trabajos automatizados
- 🚀 Se crean al inicio del job y se terminan al finalizar
- 💰 Más económicos (50% menos DBUs que all-purpose)
- ⏱️ Optimizados para ejecuciones programadas

```python
# Configuración típica de Job Cluster
{
    "cluster_name": "etl-job-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 4,
    "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true"
    },
    "azure_attributes": {
        "availability": "SPOT_AZURE",  # Usar spot instances para ahorro
        "spot_bid_max_price": -1
    }
}
```

### Modos de Cluster

| Modo | Características | Uso Recomendado |
|------|----------------|-----------------|
| **Standard** | Sin aislamiento entre usuarios | Notebooks Python/SQL de confianza |
| **High Concurrency** | Recursos compartidos eficientemente | Múltiples usuarios simultáneos |
| **Single Node** | Solo driver, sin workers | Desarrollo ligero, testing |

### Configuraciones Importantes

#### Autoscaling

```python
# El cluster escala automáticamente según la carga
autoscale = {
    "min_workers": 2,   # Mínimo para mantener disponibilidad
    "max_workers": 10   # Máximo para controlar costos
}

# Ventajas:
# - Ahorro de costos: solo paga por lo que usa
# - Rendimiento: escala cuando hay picos de trabajo
# - Simplicidad: no necesita ajuste manual
```

#### Auto-termination

```python
# Apagado automático para ahorrar costos
autotermination_minutes = 30

# Si el cluster está inactivo por 30 minutos, se apaga
# Evita costos por clusters olvidados encendidos
```

#### Spot Instances

```python
# Usar VMs de precio reducido (hasta 80% ahorro)
azure_attributes = {
    "availability": "SPOT_AZURE",
    "first_on_demand": 1,        # Primer nodo siempre on-demand
    "spot_bid_max_price": -1     # -1 = pagar hasta precio on-demand
}

# ⚠️ Precaución: Spot instances pueden ser revocadas
# Ideal para: Jobs que pueden reiniciarse, análisis no críticos
# NO ideal para: Jobs críticos de producción
```

### Políticas de Cluster

Las **Cluster Policies** permiten a los administradores controlar y estandarizar las configuraciones:

```json
{
  "cluster_type": {
    "type": "fixed",
    "value": "all-purpose"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 120
  },
  "spark_version": {
    "type": "regex",
    "pattern": "13\\..*"
  }
}
```

---

## Notebooks

### ¿Qué es un Notebook?

Un **Notebook** es un documento interactivo que combina:
- 💻 Código ejecutable (Python, Scala, SQL, R)
- 📊 Visualizaciones
- 📝 Documentación en Markdown
- 🔢 Resultados de ejecución

### Estructura de un Notebook

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Análisis de Ventas - ETL Pipeline
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Procesar datos de ventas desde Cosmos DB y generar métricas agregadas
# MAGIC 
# MAGIC ## Fuentes de Datos
# MAGIC - Cosmos DB: ventas transaccionales
# MAGIC - ADLS Gen2: datos de productos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuración Inicial

# COMMAND ----------

# Importar bibliotecas
from pyspark.sql.functions import col, sum, avg, count, date_format
from datetime import datetime

# Configurar variables
storage_account = "mystorageaccount"
container = "rawdata"
cosmos_endpoint = dbutils.secrets.get("keyvault", "cosmos-endpoint")

print(f"Pipeline iniciado: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Lectura de Datos

# COMMAND ----------

# Leer desde Cosmos DB
df_ventas = spark.read \
    .format("cosmos.oltp") \
    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .option("spark.cosmos.database", "sales") \
    .option("spark.cosmos.container", "transactions") \
    .load()

display(df_ventas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Transformaciones

# COMMAND ----------

# Agregaciones por producto
ventas_por_producto = df_ventas \
    .groupBy("producto_id", "categoria") \
    .agg(
        sum("monto").alias("ventas_totales"),
        count("*").alias("num_transacciones"),
        avg("monto").alias("ticket_promedio")
    ) \
    .orderBy(col("ventas_totales").desc())

display(ventas_por_producto)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Escritura de Resultados

# COMMAND ----------

# Guardar en Delta Lake
ventas_por_producto.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/mnt/gold/ventas_agregadas")

print("✅ Datos escritos exitosamente")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear tabla Delta para consultas SQL
# MAGIC CREATE TABLE IF NOT EXISTS gold.ventas_agregadas
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/ventas_agregadas'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar datos
# MAGIC SELECT * FROM gold.ventas_agregadas
# MAGIC ORDER BY ventas_totales DESC
# MAGIC LIMIT 20
```

### Comandos Mágicos (Magic Commands)

| Comando | Descripción | Ejemplo |
|---------|-------------|---------|
| `%python` | Ejecutar código Python | `%python print("Hola")` |
| `%scala` | Ejecutar código Scala | `%scala val x = 10` |
| `%sql` | Ejecutar SQL | `%sql SELECT * FROM tabla` |
| `%r` | Ejecutar código R | `%r plot(x, y)` |
| `%md` | Markdown | `%md # Título` |
| `%sh` | Comandos shell | `%sh ls -la` |
| `%fs` | Operaciones en DBFS | `%fs ls /mnt/data` |
| `%run` | Ejecutar otro notebook | `%run ./Utils/config` |

### Widgets: Parámetros Interactivos

```python
# Crear widgets para parametrizar notebooks
dbutils.widgets.text("fecha_inicio", "2026-01-01", "Fecha Inicio")
dbutils.widgets.dropdown("ambiente", "dev", ["dev", "test", "prod"], "Ambiente")
dbutils.widgets.multiselect("regiones", "Norte", ["Norte", "Sur", "Este", "Oeste"], "Regiones")

# Obtener valores
fecha_inicio = dbutils.widgets.get("fecha_inicio")
ambiente = dbutils.widgets.get("ambiente")
regiones = dbutils.widgets.get("regiones").split(",")

print(f"Procesando datos desde: {fecha_inicio}")
print(f"Ambiente: {ambiente}")
print(f"Regiones: {regiones}")

# Limpiar widgets al final
# dbutils.widgets.removeAll()
```

### Mejores Prácticas para Notebooks

```python
# ✅ BUENAS PRÁCTICAS

# 1. Usar celdas Markdown para documentar
# MAGIC %md
# MAGIC ## Esta sección procesa datos de clientes
# MAGIC - Filtra registros inválidos
# MAGIC - Aplica reglas de negocio
# MAGIC - Genera métricas agregadas

# 2. Modularizar código
def validar_datos(df):
    """Valida que los datos cumplan reglas de negocio"""
    return df.filter(
        (col("monto") > 0) & 
        (col("cliente_id").isNotNull())
    )

# 3. Usar display() para visualizar DataFrames
display(df_ventas.groupBy("categoria").count())

# 4. Capturar errores
try:
    df_resultado = procesar_datos(df_input)
    print("✅ Procesamiento exitoso")
except Exception as e:
    print(f"❌ Error: {str(e)}")
    dbutils.notebook.exit("FAILED")

# 5. Retornar resultados para orquestación
dbutils.notebook.exit("SUCCESS")
```

---

## Jobs

### ¿Qué es un Job?

Un **Job** es una tarea automatizada que ejecuta uno o más notebooks o scripts en un horario definido o por un trigger específico.

### Estructura de un Job

```
┌────────────────────────────────────────────────┐
│             JOB: ETL_Daily_Pipeline            │
├────────────────────────────────────────────────┤
│                                                │
│  Trigger: Schedule (Daily at 2:00 AM)         │
│  Cluster: New Job Cluster                     │
│  Timeout: 3 hours                             │
│  Max Retries: 2                               │
│                                                │
│  ┌─────────────────────────────────────────┐  │
│  │  Task 1: Ingestion                      │  │
│  │  Notebook: /ETL/01_ingest_data          │  │
│  │  Parameters: {date: "{{current_date}}"} │  │
│  │  └─────────┬───────────────────────────┘  │
│  │            │ On Success                    │
│  │  ┌─────────▼───────────────────────────┐  │
│  │  │  Task 2: Transformation             │  │
│  │  │  Notebook: /ETL/02_transform_data   │  │
│  │  │  Depends on: Task 1                 │  │
│  │  └─────────┬───────────────────────────┘  │
│  │            │ On Success                    │
│  │  ┌─────────▼───────────────────────────┐  │
│  │  │  Task 3: Aggregation                │  │
│  │  │  Notebook: /ETL/03_aggregate_data   │  │
│  │  │  Depends on: Task 2                 │  │
│  │  └─────────┬───────────────────────────┘  │
│  │            │ On Success                    │
│  │  ┌─────────▼───────────────────────────┐  │
│  │  │  Task 4: Export                     │  │
│  │  │  Notebook: /ETL/04_export_results   │  │
│  │  │  Depends on: Task 3                 │  │
│  │  └─────────────────────────────────────┘  │
│                                                │
│  Notifications:                                │
│  - On Success: team@empresa.com               │
│  - On Failure: oncall@empresa.com             │
│                                                │
└────────────────────────────────────────────────┘
```

### Configuración de Job via UI

```json
{
  "name": "ETL_Daily_Sales",
  "tasks": [
    {
      "task_key": "ingest",
      "notebook_task": {
        "notebook_path": "/Production/01_Ingest",
        "base_parameters": {
          "date": "{{job.start_time.iso_date}}",
          "source": "cosmos"
        }
      },
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2
      },
      "timeout_seconds": 3600,
      "max_retries": 2,
      "retry_on_timeout": true
    },
    {
      "task_key": "transform",
      "depends_on": [{"task_key": "ingest"}],
      "notebook_task": {
        "notebook_path": "/Production/02_Transform"
      },
      "timeout_seconds": 7200
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Mexico_City",
    "pause_status": "UNPAUSED"
  },
  "email_notifications": {
    "on_success": ["data-team@empresa.com"],
    "on_failure": ["oncall@empresa.com"],
    "no_alert_for_skipped_runs": false
  },
  "max_concurrent_runs": 1
}
```

### Tipos de Triggers

| Trigger | Descripción | Ejemplo de Uso |
|---------|-------------|----------------|
| **Scheduled** | Ejecución periódica (cron) | ETL diario a las 2 AM |
| **File Arrival** | Al detectar nuevos archivos | Procesar cuando lleguen CSVs |
| **Manual** | Ejecutado por usuario | Testing, troubleshooting |
| **API** | Llamado via REST API | Integración con sistemas externos |
| **Continuous** | Streaming continuo | Procesamiento en tiempo real |

### Expresiones Cron

```bash
# Formato: segundo minuto hora día mes día_semana

# Ejemplos:
0 0 2 * * ?         # Diario a las 2:00 AM
0 */30 * * * ?      # Cada 30 minutos
0 0 0 * * MON       # Cada lunes a medianoche
0 0 9 1 * ?         # Primer día del mes a las 9 AM
0 0 18 * * MON-FRI  # Lunes a viernes a las 6 PM
```

### Monitoreo y Alertas

```python
# Dentro del notebook, enviar métricas y alertas
from datetime import datetime

# Al inicio del job
start_time = datetime.now()
print(f"🚀 Job iniciado: {start_time}")

try:
    # Tu lógica de procesamiento
    df_result = procesar_datos()
    num_registros = df_result.count()
    
    # Validar resultados
    if num_registros == 0:
        raise ValueError("⚠️ No se procesaron registros")
    
    # Registrar métricas
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"✅ Job completado exitosamente")
    print(f"📊 Registros procesados: {num_registros}")
    print(f"⏱️ Duración: {duration}s")
    
    # Retornar éxito
    dbutils.notebook.exit(json.dumps({
        "status": "SUCCESS",
        "records": num_registros,
        "duration_seconds": duration
    }))
    
except Exception as e:
    print(f"❌ Error en el job: {str(e)}")
    # Notificar error
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": str(e)
    }))
```

---

## Bibliotecas y Dependencias

### Instalación de Librerías

#### 1. A nivel de Cluster (Todas las notebooks)

```python
# En la configuración del cluster → Libraries
# Instalar desde PyPI:
pandas==2.0.3
numpy==1.24.3
scikit-learn==1.3.0

# Instalar desde archivo .whl:
dbfs:/FileStore/libraries/my_custom_lib-1.0.0-py3-none-any.whl

# Instalar desde Maven (para Scala/Java):
com.microsoft.azure:azure-cosmos-spark_3-4_2-12:4.17.2
```

#### 2. A nivel de Notebook (Solo esa notebook)

```python
# COMMAND ----------

# Instalar con %pip (recomendado)
%pip install azure-storage-blob==12.19.0
%pip install requests==2.31.0

# Importar después de instalar
import azure.storage.blob as blob
import requests

# COMMAND ----------

# O con dbutils (método antiguo)
dbutils.library.install("PyPI", "package_name==version")
dbutils.library.restartPython()  # Requerido para cargar la librería
```

### Librerías Preinstaladas

Azure Databricks incluye cientos de librerías preinstaladas:

```python
# Ver librerías instaladas
%pip list

# Librerías comunes ya disponibles:
# - PySpark
# - Pandas
# - NumPy
# - Matplotlib
# - Scikit-learn
# - TensorFlow
# - PyTorch
# - MLflow
# - Delta Lake
```

---

## DBFS - Databricks File System

### ¿Qué es DBFS?

**DBFS** (Databricks File System) es un sistema de archivos distribuido montado en el workspace de Databricks.

### Estructura de DBFS

```
dbfs:/
├── databricks-datasets/       # Datasets de ejemplo
├── FileStore/
│   ├── tables/               # Tablas temporales
│   ├── jars/                 # Librerías Java/Scala
│   └── files/                # Archivos generales
├── mnt/                      # Puntos de montaje externos
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── tmp/                      # Archivos temporales
└── user/
    └── hive/
        └── warehouse/        # Tablas Hive
```

### Operaciones en DBFS

```python
# COMMAND ----------

# Listar archivos
dbutils.fs.ls("dbfs:/mnt/bronze/")

# Crear directorio
dbutils.fs.mkdirs("dbfs:/mnt/gold/ventas/")

# Copiar archivo
dbutils.fs.cp("dbfs:/tmp/file.csv", "dbfs:/mnt/bronze/file.csv")

# Mover/Renombrar
dbutils.fs.mv("dbfs:/mnt/bronze/old.csv", "dbfs:/mnt/bronze/new.csv")

# Eliminar archivo
dbutils.fs.rm("dbfs:/tmp/file.csv")

# Eliminar directorio recursivamente
dbutils.fs.rm("dbfs:/tmp/old_data/", recurse=True)

# Leer archivo como texto
content = dbutils.fs.head("dbfs:/mnt/config.json")
print(content)

# COMMAND ----------

# Alternativa con %fs (magic command)
%fs ls /mnt/bronze/

%fs mkdirs /mnt/gold/reports/

%fs rm -r /tmp/old_files/
```

### Montaje de Azure Storage

```python
# Montar Azure Data Lake Storage Gen2
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": dbutils.secrets.get("keyvault", "client-id"),
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("keyvault", "client-secret"),
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Montar
dbutils.fs.mount(
  source = "abfss://container@storageaccount.dfs.core.windows.net/",
  mount_point = "/mnt/datalake",
  extra_configs = configs
)

# Verificar montaje
dbutils.fs.ls("/mnt/datalake")

# Desmontar (si es necesario)
# dbutils.fs.unmount("/mnt/datalake")
```

---

## 🎯 Resumen

| Componente | Propósito | Análogo |
|------------|-----------|---------|
| **Workspace** | Entorno colaborativo | Visual Studio Workspace |
| **Cluster** | Cómputo distribuido | Servidor de aplicaciones escalable |
| **Notebook** | Desarrollo interactivo | Jupyter Notebook |
| **Job** | Automatización | Cron jobs, Azure Data Factory |
| **Library** | Código reutilizable | NuGet packages, npm |
| **DBFS** | Almacenamiento | File system compartido |

---

**Anterior**: [01 - Introducción a Databricks](./01-introduccion-databricks.md)  
**Siguiente**: [03 - Arquitectura e Integración](./03-arquitectura-integracion.md)
