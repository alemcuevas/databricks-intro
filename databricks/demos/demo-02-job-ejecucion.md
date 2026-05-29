# Demo 02 - Ejecución de Job y Revisión de Resultados

## 🎯 Objetivo de la Demo

Demostrar cómo:
- Configurar un Job desde la UI
- Ejecutar un Job manualmente
- Monitorear la ejecución en tiempo real
- Revisar resultados y logs
- Interpretar métricas de rendimiento
- Identificar y resolver errores

## ⏱️ Duración

20 minutos

---

## Parte 1: Configuración del Job (5 minutos)

### Paso 1.1: Navegar a Workflows

1. En el sidebar izquierdo, hacer clic en **Workflows**

2. Hacer clic en **Create Job** (botón azul)

### Paso 1.2: Configuración Básica del Job

```
┌──────────────────────────────────────────────────┐
│  Create Job                                      │
├──────────────────────────────────────────────────┤
│                                                  │
│  Job name:                                       │
│  ┌────────────────────────────────────────────┐  │
│  │ demo_etl_daily_sales                       │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  Schedule:                                       │
│  ☑ Run every day at 3:00 AM (America/New_York) │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │ 0 0 3 * * ?                                │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  [Advanced scheduling options]                   │
│                                                  │
└──────────────────────────────────────────────────┘
```

**Configuración recomendada**:
- **Job name**: `demo_etl_daily_sales`
- **Schedule**: Manual (para demo), o `0 0 3 * * ?` para diario a las 3 AM
- **Timezone**: America/New_York (o tu timezone)

### Paso 1.3: Agregar Tasks

```
┌──────────────────────────────────────────────────┐
│  Tasks                                           │
├──────────────────────────────────────────────────┤
│                                                  │
│  [+ Add Task]                                    │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │  📝 Task 1: ingest_data                    │  │
│  │  Type: Notebook                            │  │
│  │  Source: /Users/you@email.com/ETL/ingest  │  │
│  │  Cluster: New Job Cluster                  │  │
│  │  ──────────────────────────────────────    │  │
│  │  On success: → transform_data              │  │
│  └────────────────────────────────────────────┘  │
│                      │                           │
│                      ▼                           │
│  ┌────────────────────────────────────────────┐  │
│  │  📝 Task 2: transform_data                 │  │
│  │  Type: Notebook                            │  │
│  │  Source: /Users/you@email.com/ETL/        │  │
│  │  Depends on: ingest_data                   │  │
│  │  ──────────────────────────────────────    │  │
│  │  On success: → export_results              │  │
│  └────────────────────────────────────────────┘  │
│                      │                           │
│                      ▼                           │
│  ┌────────────────────────────────────────────┐  │
│  │  📝 Task 3: export_results                 │  │
│  │  Type: Notebook                            │  │
│  │  Depends on: transform_data                │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
└──────────────────────────────────────────────────┘
```

**Para cada Task**:

1. Click en **+ Add Task**

2. Configurar:
   - **Task name**: `ingest_data`
   - **Type**: Notebook
   - **Source**: Seleccionar notebook
   - **Cluster**: New Job Cluster (configurar después)

3. En **Advanced Options**:
   - **Timeout**: 3600 segundos (1 hora)
   - **Max retries**: 2
   - **Retry on timeout**: ☑ Enabled

### Paso 1.4: Configurar Job Cluster

```
┌──────────────────────────────────────────────────┐
│  Compute                                         │
├──────────────────────────────────────────────────┤
│                                                  │
│  Cluster configuration:                          │
│                                                  │
│  Node type:          Standard_DS3_v2             │
│  Workers:            ☑ Autoscaling 2-8          │
│  Databricks Runtime: 13.3 LTS                    │
│                                                  │
│  Advanced options:                               │
│  ☑ Enable autoscaling                           │
│  ☑ Use spot instances (workers only)            │
│  ☑ Optimize writes for Delta                    │
│                                                  │
│  Estimated cost: $1.50 - $6.00 per run           │
│                                                  │
└──────────────────────────────────────────────────┘
```

### Paso 1.5: Configurar Alertas

```
┌──────────────────────────────────────────────────┐
│  Alerts                                          │
├──────────────────────────────────────────────────┤
│                                                  │
│  On success:                                     │
│  ☑ Send email to: data-team@empresa.com         │
│                                                  │
│  On failure:                                     │
│  ☑ Send email to: oncall@empresa.com            │
│  ☑ Don't alert for skipped runs                 │
│                                                  │
│  Alert on duration exceeded:                     │
│  ☑ Alert if run exceeds 45 minutes              │
│                                                  │
└──────────────────────────────────────────────────┘
```

### Paso 1.6: Agregar Parámetros

```python
# En el job, agregar parámetros que se pasarán a los notebooks

Parameters:
{
    "fecha_proceso": "2026-05-22",
    "environment": "demo",
    "region": "all",
    "debug_mode": "false"
}
```

**Guardar el Job**: Click en **Create**

---

## Parte 2: Ejecución Manual del Job (5 minutos)

### Paso 2.1: Ejecutar el Job

1. En la página del Job, hacer clic en **Run now** (botón azul)

2. Opcionalmente, sobrescribir parámetros:
   ```
   ┌────────────────────────────────────────┐
   │  Run now with parameters              │
   ├────────────────────────────────────────┤
   │                                        │
   │  fecha_proceso: 2026-05-22            │
   │  environment:   demo                   │
   │  region:        all                    │
   │  debug_mode:    true      ← Override  │
   │                                        │
   │  [Cancel]  [Confirm]                  │
   │                                        │
   └────────────────────────────────────────┘
   ```

3. Click en **Confirm**

### Paso 2.2: Vista de Ejecución

La UI mostrará:

```
┌──────────────────────────────────────────────────────────┐
│  demo_etl_daily_sales                                    │
│  Run #42 - Started at 2026-05-22 14:30:15               │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Status: ⚙️ Running    Duration: 00:02:34               │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📗 ingest_data              ✅ SUCCESS (1m 20s)  │  │
│  └────────────────────────────────────────────────────┘  │
│                      │                                   │
│                      ▼                                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  ⚙️ transform_data           🔄 RUNNING (1m 14s) │  │
│  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━░░░░░░░  60%  │  │
│  └────────────────────────────────────────────────────┘  │
│                      │                                   │
│                      ▼                                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📘 export_results           ⏸️ PENDING           │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  Cluster: job-42-run-1 (2 workers) 🟢 Running           │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

**Observar**:
- ✅ Tasks completadas muestran checkmark verde
- 🔄 Task actual muestra spinner y progreso
- ⏸️ Tasks pendientes muestran icono gris
- ⏱️ Duración de cada task
- 📊 Progreso general del job

---

## Parte 3: Monitoreo en Tiempo Real (5 minutos)

### Paso 3.1: Ver Detalles de un Task

1. Hacer clic en el task **transform_data** (el que está corriendo)

2. Se abre una vista detallada:

```
┌──────────────────────────────────────────────────────────┐
│  Task: transform_data                                    │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Status: 🔄 Running    Duration: 00:01:45                │
│  Cluster: job-42-run-1                                   │
│  Start time: 2026-05-22 14:31:35                         │
│                                                          │
│  ┌─────────────────────────────────────────────────┐     │
│  │ 📓 Notebook Output                              │     │
│  │                                                 │     │
│  │ Cell 1: Configuration ✅                        │     │
│  │ Cell 2: Read from Bronze ✅                     │     │
│  │ Cell 3: Transform data 🔄 (2m 10s)             │     │
│  │    Processing 1,245,678 records...             │     │
│  │    Progress: ████████████░░░░░░░░ 65%          │     │
│  │                                                 │     │
│  └─────────────────────────────────────────────────┘     │
│                                                          │
│  [View Full Notebook Output]                             │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Paso 3.2: Ver Logs del Cluster

1. Hacer clic en la pestaña **Cluster Logs**

```
[2026-05-22 14:32:15] INFO: Starting Spark job
[2026-05-22 14:32:16] INFO: Reading from Delta table: /mnt/bronze/sales
[2026-05-22 14:32:20] INFO: Loaded 1,245,678 records
[2026-05-22 14:32:25] INFO: Applying transformations...
[2026-05-22 14:33:10] INFO: Transformation stage 1 complete: 1,240,123 valid records
[2026-05-22 14:33:15] WARN: Filtered out 5,555 invalid records
[2026-05-22 14:33:45] INFO: Writing to Delta table: /mnt/silver/sales
```

**Qué buscar en los logs**:
- ✅ INFO messages: progreso normal
- ⚠️ WARN messages: posibles issues
- ❌ ERROR messages: problemas críticos
- 📊 Contadores de registros

### Paso 3.3: Ver Métricas de Spark

1. Hacer clic en **Spark UI**

```
┌──────────────────────────────────────────────────┐
│  Spark UI - Job Details                          │
├──────────────────────────────────────────────────┤
│                                                  │
│  Job 0: read at DataFrame.scala:215              │
│  Duration: 4.5 s                                 │
│  Stages: 1                                       │
│  Tasks: 200 (200 succeeded)                      │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │  Stage 0: Read Delta                       │  │
│  │  Duration: 4.5 s                           │  │
│  │  Input: 2.3 GB                             │  │
│  │  Output: 1.2 million records               │  │
│  │  ──────────────────────────────────────    │  │
│  │  Task metrics:                             │  │
│  │    Min: 18 ms                              │  │
│  │    Median: 22 ms                           │  │
│  │    Max: 45 ms                              │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
└──────────────────────────────────────────────────┘
```

**Métricas importantes**:
- **Duration**: Tiempo total del job
- **Input/Output**: Cantidad de datos procesados
- **Shuffle Read/Write**: Datos movidos entre nodos
- **Task Time**: Distribución de tiempo entre tasks

### Paso 3.4: Monitorear Uso de Recursos

```
┌──────────────────────────────────────────────────┐
│  Cluster Metrics                                 │
├──────────────────────────────────────────────────┤
│                                                  │
│  CPU Usage:      ████████████░░░░░░ 65%         │
│  Memory Usage:   ██████████░░░░░░░░ 55%         │
│  Disk I/O:       ████████████████░░ 80%         │
│  Network:        ████░░░░░░░░░░░░░░ 20%         │
│                                                  │
│  Active Workers: 4 / 8 (autoscaling)             │
│                                                  │
└──────────────────────────────────────────────────┘
```

---

## Parte 4: Revisión de Resultados (5 minutos)

### Paso 4.1: Job Completado

Cuando el job termina, verás:

```
┌──────────────────────────────────────────────────────────┐
│  demo_etl_daily_sales                                    │
│  Run #42 - Completed at 2026-05-22 14:38:22             │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Status: ✅ SUCCESS    Duration: 00:08:07                │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📗 ingest_data              ✅ (1m 20s)          │  │
│  └────────────────────────────────────────────────────┘  │
│                      │                                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📗 transform_data           ✅ (4m 35s)          │  │
│  └────────────────────────────────────────────────────┘  │
│                      │                                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📗 export_results           ✅ (2m 12s)          │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  💰 Cluster cost: $1.23                                  │
│  📧 Notification sent to: data-team@empresa.com          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Paso 4.2: Ver Resultados de Cada Task

Hacer clic en cada task para ver su output:

**Task 1: ingest_data**
```python
# Output del notebook:

✅ Configuración cargada:
   - Fecha de proceso: 2026-05-22
   - Storage account: mystorageaccount
   
📊 Registros ingested:
   - Ventas Online: 125,432 registros
   - Ventas POS: 89,567 registros
   - TOTAL: 214,999 registros

✅ Datos guardados en Bronze
```

**Task 2: transform_data**
```python
# Output del notebook:

🧹 Limpiando datos...
   Iniciales: 214,999
   Duplicados removidos: 1,234
   Inválidos removidos: 3,456
   Finales (limpios): 210,309

📊 Transformaciones aplicadas:
   - Estandarización de esquemas
   - Normalización de texto
   - Enriquecimiento con catálogo
   
✅ Datos guardados en Silver
```

**Task 3: export_results**
```python
# Output del notebook:

📈 Métricas calculadas:
   - Ventas totales: $1,245,678.90
   - Transacciones: 210,309
   - Ticket promedio: $59.21
   - Clientes únicos: 45,678

✅ Datos exportados a:
   - Cosmos DB: daily_sales_summary (42 documentos)
   - Delta Gold: /mnt/gold/sales_daily_metrics/
   
⏱️ Duración total: 8m 7s
```

### Paso 4.3: Verificar Datos en Gold Layer

```sql
-- Ejecutar en un notebook SQL

SELECT 
    region,
    category,
    total_sales,
    num_transactions,
    unique_customers
FROM gold.sales_daily_metrics
WHERE sale_date = '2026-05-22'
ORDER BY total_sales DESC
LIMIT 10;
```

Resultado:
```
+--------+-------------+-------------+------------------+-----------------+
| region | category    | total_sales | num_transactions | unique_customers|
+--------+-------------+-------------+------------------+-----------------+
| NORTE  | Tecnología  | 456,789.12  | 3,456            | 2,345          |
| SUR    | Tecnología  | 234,567.89  | 2,345            | 1,567          |
| ESTE   | Hogar       | 198,765.43  | 4,567            | 3,456          |
| OESTE  | Tecnología  | 156,789.01  | 1,234            | 987            |
+--------+-------------+-------------+------------------+-----------------+
```

### Paso 4.4: Historial de Ejecuciones

Ver el historial completo:

```
┌──────────────────────────────────────────────────────────────┐
│  Run History                                                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Run #  Status      Started            Duration   Cost      │
│  ──────────────────────────────────────────────────────────  │
│  #42    ✅ SUCCESS  2026-05-22 14:30   8m 7s      $1.23     │
│  #41    ✅ SUCCESS  2026-05-21 03:00   7m 45s     $1.18     │
│  #40    ✅ SUCCESS  2026-05-20 03:00   8m 12s     $1.25     │
│  #39    ❌ FAILED   2026-05-19 03:00   2m 34s     $0.45     │
│  #38    ✅ SUCCESS  2026-05-18 03:00   7m 56s     $1.21     │
│                                                              │
│  Average duration: 7m 58s                                    │
│  Success rate: 80% (last 10 runs)                           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Analizar**:
- ✅ Tasa de éxito
- ⏱️ Duración promedio y variabilidad
- 💰 Costo por ejecución
- 📊 Tendencias (está mejorando o empeorando?)

---

## Parte 5: Troubleshooting (solo si aplica)

### Escenario: Job Falló

Si un job falla, verás:

```
┌──────────────────────────────────────────────────────────┐
│  demo_etl_daily_sales                                    │
│  Run #43 - Failed at 2026-05-22 15:15:43                │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Status: ❌ FAILED    Duration: 00:03:21                 │
│  Error: Notebook execution failed                        │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📗 ingest_data              ✅ (1m 18s)          │  │
│  └────────────────────────────────────────────────────┘  │
│                      │                                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📕 transform_data           ❌ FAILED (2m 3s)    │  │
│  │                                                    │  │
│  │  Error: KeyError: 'product_id'                    │  │
│  │  Cell 15, line 8                                  │  │
│  └────────────────────────────────────────────────────┘  │
│                      │                                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  📘 export_results           ⏭️ SKIPPED          │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  📧 Notification sent to: oncall@empresa.com             │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Pasos para Troubleshooting

1. **Hacer clic en el task fallido** para ver detalles

2. **Revisar el error**:
   ```python
   KeyError: 'product_id'
   File "/databricks/python/lib/python3.10/site-packages/pandas/core/indexes/base.py", line 3805, in get_loc
       return self._engine.get_loc(casted_key)
   File "/databricks/python/lib/python3.10/site-packages/pandas/_libs/index.pyx", line 138, in pandas._libs.index.IndexEngine.get_loc
   File "/databricks/python/lib/python3.10/site-packages/pandas/_libs/index.pyx", line 165, in pandas._libs.index.IndexEngine.get_loc
   KeyError: 'product_id'
   ```

3. **Ver el notebook output** en el punto de fallo:
   ```python
   # Cell 15 - Enrich with product catalog
   df_enriched = df.select("product_id", "quantity", "price")
   
   # ERROR: La columna 'product_id' no existe en el DataFrame
   # Probablemente fue renombrada en la ingesta
   ```

4. **Revisar logs del cluster**:
   ```
   [2026-05-22 15:13:40] INFO: Reading from Bronze...
   [2026-05-22 15:13:45] INFO: Schema: [prod_id, qty, price, timestamp]
   [2026-05-22 15:14:12] INFO: Applying transformations...
   [2026-05-22 15:14:35] ERROR: Column 'product_id' not found
   [2026-05-22 15:14:35] ERROR: Available columns: ['prod_id', 'qty', 'price', 'timestamp']
   ```

5. **Identificar el problema**:
   - ❌ El notebook espera `product_id`
   - ✅ Los datos tienen `prod_id`
   - 🔧 Solución: Actualizar el código o la ingesta

6. **Corregir y re-ejecutar**:
   - Editar el notebook
   - Re-ejecutar el job (botón **Repair Run**)

---

## 🎯 Puntos Clave de la Demo

Durante la demostración, enfatiza:

### 1. Configuración del Job
- ✅ Nombres descriptivos
- ✅ Dependencias claras entre tasks
- ✅ Timeouts y retries configurados
- ✅ Alertas apropiadas

### 2. Monitoreo
- ✅ Vista en tiempo real del progreso
- ✅ Logs detallados por task
- ✅ Métricas de Spark
- ✅ Uso de recursos del cluster

### 3. Resultados
- ✅ Verificar output de cada task
- ✅ Validar datos en destino
- ✅ Revisar métricas de negocio
- ✅ Confirmar notificaciones enviadas

### 4. Troubleshooting
- ✅ Identificar rápidamente el task fallido
- ✅ Leer y entender mensajes de error
- ✅ Revisar logs del cluster
- ✅ Reparar y re-ejecutar

---

## ❓ Preguntas para Hacer al Partner

Durante la demo, preguntar:

### Sobre Configuración
1. ¿Qué jobs tienen en producción actualmente?
2. ¿Cómo decidieron la configuración de clusters?
3. ¿Por qué eligieron esos schedules específicos?
4. ¿Hay dependencias entre jobs diferentes?

### Sobre Monitoreo
5. ¿Quién monitorea los jobs diariamente?
6. ¿Cómo se enteran cuando algo falla?
7. ¿Revisan los logs regularmente?
8. ¿Hay dashboards de monitoreo adicionales?

### Sobre Troubleshooting
9. ¿Cuáles son los errores más comunes?
10. ¿Cómo los resuelven típicamente?
11. ¿Hay runbooks documentados?
12. ¿Cuánto tiempo toma típicamente resolver un issue?

### Sobre Operación
13. ¿Hay ventanas de mantenimiento?
14. ¿Cómo manejan re-ejecuciones?
15. ¿Se monitorea el costo de los jobs?
16. ¿Hay optimizaciones planificadas?

---

## 📚 Recursos Adicionales

- [Documentación: Jobs](https://docs.microsoft.com/azure/databricks/jobs/)
- [Monitoring Jobs](https://docs.microsoft.com/azure/databricks/jobs/monitor)
- [Troubleshooting Guide](https://docs.microsoft.com/azure/databricks/kb/jobs/jobs-troubleshooting)

---

**Anterior**: [Demo 01 - Notebook de Transformación](./demo-01-notebook-transformacion.md)
