# Preguntas Frecuentes (FAQ) - Azure Databricks

## 📚 Índice

- [General](#general)
- [Clusters](#clusters)
- [Notebooks](#notebooks)
- [Jobs](#jobs)
- [Datos y Storage](#datos-y-storage)
- [Costos](#costos)
- [Seguridad](#seguridad)
- [Troubleshooting](#troubleshooting)
- [Rendimiento](#rendimiento)

---

## General

### ¿Qué es Azure Databricks?

Azure Databricks es una plataforma de análisis de datos basada en Apache Spark, optimizada para Azure. Combina lo mejor de Databricks y Microsoft Azure para proporcionar:
- Procesamiento de datos a gran escala
- Machine Learning integrado
- Colaboración en equipo
- Integración nativa con servicios Azure

### ¿Cuál es la diferencia entre Databricks y Apache Spark?

| Característica | Apache Spark (Open Source) | Azure Databricks |
|----------------|---------------------------|------------------|
| **Motor de procesamiento** | ✅ Apache Spark | ✅ Apache Spark optimizado |
| **UI amigable** | ❌ Limitada | ✅ Workspace colaborativo |
| **Gestión de clusters** | ❌ Manual | ✅ Automatizada |
| **Optimizaciones** | ❌ Stock | ✅ Delta Lake, Photon |
| **Integración Azure** | ❌ Manual | ✅ Nativa |
| **Soporte** | ❌ Comunidad | ✅ Microsoft + Databricks |

### ¿Cuándo debo usar Databricks vs Azure Synapse vs Azure Data Factory?

**Usa Databricks cuando**:
- Necesitas procesamiento complejo de datos (transformaciones avanzadas)
- Trabajas con big data (TB o PB)
- Requieres Machine Learning
- Necesitas flexibilidad con Python/Scala/SQL
- Tienes científicos de datos en el equipo

**Usa Synapse cuando**:
- Principalmente queries SQL
- Data warehouse tradicional
- Integración fuerte con Power BI
- Usuarios más orientados a SQL

**Usa Data Factory cuando**:
- Orquestación simple ETL
- Mover datos entre servicios
- Transformaciones básicas
- No necesitas procesamiento complejo

---

## Clusters

### ¿Cuál es la diferencia entre All-Purpose y Job clusters?

| Aspecto | All-Purpose | Job Cluster |
|---------|-------------|-------------|
| **Uso** | Desarrollo interactivo | Producción automatizada |
| **Costo** | ~$0.55/DBU | ~$0.22/DBU (50% menos) |
| **Lifetime** | Persistente | Se destruye al terminar |
| **Compartido** | ✅ Múltiples usuarios | ❌ Un job específico |
| **Cuándo usar** | Exploración, desarrollo | Jobs programados |

**Regla de oro**: Desarrolla en All-Purpose, ejecuta en Job clusters.

### ¿Por qué mi cluster tarda tanto en iniciar?

Tiempo típico de inicio: **3-5 minutos**

Causas de demoras:
- **VM provisioning** (Azure crea las VMs): 1-2 min
- **Docker image download**: 1-2 min
- **Databricks runtime init**: 1-2 min
- **Library installation**: Variable (puede añadir minutos)

**Soluciones**:
- Usar **Cluster Pools**: reduce inicio a ~30 segundos
- Mantener cluster corriendo con **Auto-termination** adecuado
- Evitar instalar muchas librerías en startup

### ¿Qué tamaño de cluster necesito?

**Regla general**:

| Volumen de datos | Workers | VM Type | Ejemplo |
|------------------|---------|---------|---------|
| < 10 GB | 2 | Standard_DS3_v2 | Desarrollo |
| 10-100 GB | 2-4 | Standard_DS3_v2 | ETL ligero |
| 100 GB - 1 TB | 4-8 | Standard_DS4_v2 | ETL medio |
| 1-10 TB | 8-16 | Standard_DS5_v2 | ETL pesado |
| > 10 TB | 16+ | Standard_E8s_v3 | Big data |

**Consejo**: Empieza pequeño, escala según necesidad. Usa **autoscaling** para flexibilidad.

### ¿Debo usar Spot instances?

**Ventajas**:
- ✅ Hasta 80% de ahorro
- ✅ Perfecto para jobs batch que pueden tolerar interrupciones
- ✅ Databricks maneja automáticamente las interrupciones

**Desventajas**:
- ❌ Puede ser interrumpido por Azure (raro pero posible)
- ❌ No disponible siempre en todas las regiones

**Recomendación**: 
- ✅ Usa Spot para workers en job clusters
- ❌ NO uses Spot para driver
- ❌ NO uses Spot para jobs críticos con SLA estricto

---

## Notebooks

### ¿Puedo usar Jupyter Notebooks en Databricks?

Sí, pero con diferencias:

| Característica | Jupyter | Databricks Notebook |
|----------------|---------|---------------------|
| **Formato** | .ipynb | .dbc (o .py, .sql) |
| **Magic commands** | ✅ Limitados | ✅ Extendidos (%run, %sql, etc.) |
| **Colaboración** | ❌ Difícil | ✅ Real-time |
| **Integración Spark** | ❌ Manual | ✅ Nativa |
| **Widgets/Parámetros** | ❌ No | ✅ Sí |

**Consejo**: Puedes importar .ipynb, pero aprovechar las características de Databricks.

### ¿Cómo comparto un notebook con mi equipo?

**Opción 1: Compartir acceso directo**
```
1. Abrir el notebook
2. Click en el botón de "Share" (arriba derecha)
3. Agregar emails y permisos (View/Run/Edit)
```

**Opción 2: Carpeta compartida**
```
Workspace → Shared → Crear carpeta del equipo
Mover notebooks ahí
```

**Opción 3: Control de versiones (recomendado para producción)**
```
Workspace → Repos → Conectar con Azure DevOps/GitHub
Colaborar vía Git
```

### ¿Puedo programar un notebook para que corra automáticamente?

Sí, a través de **Jobs**:

```
1. Workflows → Create Job
2. Agregar task → Type: Notebook
3. Seleccionar tu notebook
4. Configurar schedule (ej: diario a las 3 AM)
5. Asignar un Job cluster
6. Guardar y activar
```

---

## Jobs

### ¿Qué pasa si mi job falla?

Databricks ofrece:

1. **Retries automáticos**: Configurable (0-3 retries típicamente)
2. **Alertas**: Email/webhook cuando falla
3. **Repair runs**: Puedes re-ejecutar solo las tasks fallidas

**Ejemplo de configuración**:
```json
{
  "max_retries": 2,
  "timeout_seconds": 3600,
  "email_notifications": {
    "on_failure": ["oncall@empresa.com"],
    "on_success": ["data-team@empresa.com"]
  }
}
```

### ¿Cómo paso parámetros a un job?

**En la configuración del job**:
```json
{
  "parameters": {
    "fecha": "2026-05-22",
    "region": "north"
  }
}
```

**En el notebook, leer parámetros**:
```python
# Opción 1: Widgets (recomendado)
dbutils.widgets.text("fecha", "2026-01-01", "Fecha de proceso")
fecha = dbutils.widgets.get("fecha")

# Opción 2: Argumentos de notebook (legacy)
dbutils.notebook.entry_point.getCurrentBindings()["fecha"]
```

### ¿Puedo orquestar múltiples notebooks en un job?

Sí, con **multi-task jobs**:

```
Job: ETL_Completo
├── Task 1: Ingest (notebook1)
├── Task 2: Transform (notebook2) ← Depende de Task 1
└── Task 3: Export (notebook3) ← Depende de Task 2
```

También puedes crear **branching**:
```
Task 1: Ingest
├── Task 2a: Transform_Online ← En paralelo
└── Task 2b: Transform_POS    ← En paralelo
    └── Task 3: Consolidate ← Espera ambos
```

---

## Datos y Storage

### ¿Dónde debo guardar mis datos?

**Capa Bronze (raw data)**:
- Azure Data Lake Storage Gen2 (ADLS)
- Formato: Parquet o JSON
- Path: `/bronze/source_name/YYYY/MM/DD/`

**Capa Silver (cleaned)**:
- ADLS Gen2
- Formato: **Delta Lake** (recomendado)
- Path: `/silver/entity_name/`

**Capa Gold (aggregated)**:
- ADLS Gen2
- Formato: **Delta Lake**
- Path: `/gold/report_name/`

**Para serving (queries rápidas)**:
- Azure Cosmos DB (< 1 TB, latencia ultra-baja)
- Azure SQL Database (datos relacionales)

### ¿Qué es Delta Lake y por qué debería usarlo?

**Delta Lake** = Parquet + ACID transactions + time travel + optimizations

**Ventajas**:
- ✅ **ACID transactions**: Consistencia garantizada
- ✅ **Time travel**: Ver versiones anteriores de datos
- ✅ **Schema enforcement**: Previene errores de escritura
- ✅ **MERGE/UPDATE/DELETE**: Operaciones que Parquet no soporta
- ✅ **Z-Ordering**: Optimización para queries

**Ejemplo**:
```python
# Escribir Delta
df.write.format("delta").save("/mnt/delta/sales")

# Leer versión anterior (time travel)
df_yesterday = spark.read \
    .format("delta") \
    .option("versionAsOf", 1) \
    .load("/mnt/delta/sales")

# Ver historial
spark.sql("DESCRIBE HISTORY delta.`/mnt/delta/sales`")
```

### ¿Cómo conecto Databricks con Azure Storage?

**Opción 1: Access Key (simple, menos seguro)**
```python
spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    "<access-key>"
)
```

**Opción 2: Service Principal (recomendado)**
```python
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "<app-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret", "<secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", 
               "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
```

**Opción 3: Managed Identity (más seguro, sin secrets)**
```python
spark.conf.set("fs.azure.account.auth.type", "ManagedIdentity")
```

### ¿Cómo leo datos desde Cosmos DB?

```python
# Configuración
cosmos_config = {
    "spark.cosmos.accountEndpoint": "<cosmos-endpoint>",
    "spark.cosmos.accountKey": "<cosmos-key>",  # O usar KeyVault
    "spark.cosmos.database": "sales",
    "spark.cosmos.container": "transactions"
}

# Leer datos
df = spark.read \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .load()

# Leer con query específica
df_filtered = spark.read \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.read.customQuery", 
            "SELECT * FROM c WHERE c.date = '2026-05-22'") \
    .load()

# Escribir a Cosmos
df.write \
    .format("cosmos.oltp") \
    .options(**cosmos_config) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .mode("append") \
    .save()
```

---

## Costos

### ¿Cuánto cuesta Databricks?

**Componentes de costo**:

1. **DBUs (Databricks Units)**
   - Premium All-Purpose: ~$0.55/DBU
   - Premium Jobs: ~$0.22/DBU

2. **Azure Compute (VMs)**
   - Depende del tipo de VM
   - Standard_DS3_v2: ~$0.15/hora

3. **Storage**
   - ADLS Gen2: ~$0.02/GB/mes

**Cálculo de ejemplo**:
```
Job cluster con 4 workers Standard_DS3_v2
Corre 2 horas/día

DBUs: 4 workers × 2 DBUs/worker × 2 horas × 30 días = 480 DBUs
Costo DBU: 480 × $0.22 = $105.60

Compute: 4 workers × $0.15/hora × 2 horas × 30 días = $36.00

Storage: 1 TB × $0.02 = $20.00

TOTAL: $161.60/mes
```

### ¿Cómo puedo reducir costos?

**Top 5 estrategias**:

1. **Usar Job clusters en lugar de All-Purpose**: 50% ahorro
2. **Habilitar auto-termination**: Apaga clusters idle
3. **Usar Spot instances**: Hasta 80% ahorro
4. **Autoscaling**: Paga solo por lo que usas
5. **Optimize Delta tables**: Reduce tiempo de ejecución

**Implementación rápida**:
```python
# En cluster config
{
    "autoscale": {
        "min_workers": 2,
        "max_workers": 8
    },
    "aws_attributes": {
        "spot_bid_price_percent": 100  # Usa Spot
    },
    "autotermination_minutes": 30
}
```

### ¿Se cobra cuando el cluster está idle?

**Sí**, mientras el cluster esté "Running", se cobra DBUs + Compute.

**Solución**: Configurar **auto-termination**:
```
Cluster settings → Advanced Options → Auto Termination
Valor recomendado: 30-60 minutos
```

---

## Seguridad

### ¿Cómo manejo secrets/passwords?

**NUNCA** hardcodear secrets en notebooks.

**Opción 1: Azure Key Vault (recomendado)**
```python
# 1. Crear secret scope (una vez, desde UI o CLI)
# Databricks Settings → Secrets → Create Scope

# 2. Usar en notebooks
password = dbutils.secrets.get(scope="keyvault", key="db-password")
```

**Opción 2: Databricks Secrets (menos común en Azure)**
```python
# Crear secret vía CLI
# databricks secrets put --scope my-scope --key my-secret

# Usar en notebook
secret = dbutils.secrets.get(scope="my-scope", key="my-secret")
```

### ¿Quién puede ver mis notebooks?

Depende de los permisos:

| Permiso | Puede Ver | Puede Ejecutar | Puede Editar | Puede Borrar |
|---------|-----------|----------------|--------------|--------------|
| **No Access** | ❌ | ❌ | ❌ | ❌ |
| **Can View** | ✅ | ❌ | ❌ | ❌ |
| **Can Run** | ✅ | ✅ | ❌ | ❌ |
| **Can Edit** | ✅ | ✅ | ✅ | ❌ |
| **Can Manage** | ✅ | ✅ | ✅ | ✅ |

**Configurar**:
```
Notebook → Share button → Add user/group → Select permission
```

### ¿Cómo restrinjo qué tipo de clusters pueden crear los usuarios?

Usa **Cluster Policies**:

```json
{
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"],
    "defaultValue": "Standard_DS3_v2"
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 60,
    "defaultValue": 30
  },
  "spark_version": {
    "type": "regex",
    "pattern": "^13\\..*-scala.*$"  // Solo 13.x LTS
  }
}
```

**Aplicar**: Admin Console → Compute → Policies → Create

---

## Troubleshooting

### "Cluster start failed: Could not launch cluster"

**Causas comunes**:

1. **Cuota de Azure excedida**
   - Solución: Pedir aumento de cuota en Azure Portal

2. **Región sin capacidad**
   - Solución: Cambiar a otra región o VM type

3. **Política de cluster inválida**
   - Solución: Revisar y corregir la policy

4. **Networking issues** (si workspace en VNet)
   - Solución: Verificar NSG rules y UDR

### "OutOfMemoryError" en notebooks

**Causas**:
- DataFrame no particionado adecuadamente
- Collect() de dataset grande
- Broadcast join de tabla grande
- Acumulación de objetos en driver

**Soluciones**:

```python
# ❌ MAL: Collect dataset grande
df_large.collect()  # ¡NUNCA hagas esto!

# ✅ BIEN: Usar display o write
display(df_large.limit(1000))
df_large.write.format("delta").save("/path")

# ❌ MAL: Cache sin necesidad
df.cache()
df.count()
# ... nunca más se usa df

# ✅ BIEN: Cache solo lo necesario
df_filtered = df.filter(col("date") == "2026-05-22")
df_filtered.cache()  # Dataset pequeño y reutilizado
```

### Mi job es muy lento

**Diagnóstico**:

1. **Ver Spark UI** → Identificar stage lento
2. **Revisar métricas**:
   - ¿Alto shuffle read/write? → Particionar mejor
   - ¿Tasks desbalanceados? → Revisar skew
   - ¿Muchos small files? → OPTIMIZE

**Optimizaciones comunes**:

```python
# 1. Particionar por columna relevante
df.write \
    .format("delta") \
    .partitionBy("date", "region") \
    .save("/path")

# 2. Z-Ordering para queries
from delta.tables import DeltaTable
DeltaTable.forPath(spark, "/path") \
    .optimize() \
    .executeZOrderBy("product_id")

# 3. Broadcast join para tablas pequeñas
from pyspark.sql.functions import broadcast
df_result = df_large.join(
    broadcast(df_small),
    on="key"
)

# 4. Repartition antes de escribir
df.repartition(10).write.format("delta").save("/path")
```

---

## Rendimiento

### ¿Cuántas particiones debo usar?

**Regla general**: 2-4 particiones por core en tu cluster

```python
# Cluster con 4 workers × 4 cores = 16 cores
# → Usar 32-64 particiones

df = df.repartition(40)
```

**Auto-partitioning**:
```python
# Databricks ajusta automáticamente con Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### ¿Debo usar cache()?

**Usa cache() cuando**:
- ✅ El DataFrame se usa múltiples veces
- ✅ El DataFrame es resultado de transformación costosa
- ✅ Tienes memoria suficiente

```python
# ✅ BIEN: DataFrame reutilizado
df_filtered = df.filter(col("date") >= "2026-01-01")
df_filtered.cache()

df_agg1 = df_filtered.groupBy("region").count()
df_agg2 = df_filtered.groupBy("product").sum("sales")
```

**NO uses cache() cuando**:
- ❌ El DataFrame se usa solo una vez
- ❌ El DataFrame es muy grande (no cabe en memoria)
- ❌ La computación es simple (más rápido recalcular que cachear)

### ¿Cuándo usar OPTIMIZE y Z-ORDER?

**OPTIMIZE**: Consolidar archivos pequeños

```python
from delta.tables import DeltaTable

# Correr regularmente (ej: noche)
DeltaTable.forPath(spark, "/mnt/delta/sales") \
    .optimize() \
    .executeCompaction()
```

**Z-ORDER**: Optimizar queries por columnas específicas

```python
# Columnas frecuentemente usadas en WHERE
DeltaTable.forPath(spark, "/mnt/delta/sales") \
    .optimize() \
    .executeZOrderBy("region", "product_category")
```

**Frecuencia**:
- Tables con escrituras frecuentes: Diario o semanal
- Tables con pocas escrituras: Mensual

---

## Más Preguntas?

Si tienes una pregunta que no está aquí:

1. **Documentación oficial**: [docs.microsoft.com/azure/databricks](https://docs.microsoft.com/azure/databricks)
2. **Community Forums**: [community.databricks.com](https://community.databricks.com)
3. **Stack Overflow**: Tag `azure-databricks`
4. **Microsoft Q&A**: [learn.microsoft.com/answers](https://learn.microsoft.com/answers)

---

**Última actualización**: 2026-05-22
