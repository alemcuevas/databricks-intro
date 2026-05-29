# 04 - Clusters y Gestión de Costos

## 📝 Índice
- [Entendiendo los Costos en Databricks](#entendiendo-los-costos-en-databricks)
- [Tipos de Clusters y su Impacto en Costos](#tipos-de-clusters-y-su-impacto-en-costos)
- [Estrategias de Optimización de Costos](#estrategias-de-optimización-de-costos)
- [Configuraciones para Control de Costos](#configuraciones-para-control-de-costos)
- [Monitoreo de Costos](#monitoreo-de-costos)
- [Mejores Prácticas](#mejores-prácticas)

---

## Entendiendo los Costos en Databricks

### Componentes del Costo

```
┌──────────────────────────────────────────────────┐
│         COSTO TOTAL DE DATABRICKS                │
├──────────────────────────────────────────────────┤
│                                                  │
│  💰 COSTO = DBU + Compute + Storage + Networking│
│                                                  │
│  1️⃣ DBU (Databricks Units)                      │
│     • Licencia de Databricks                    │
│     • Por hora de ejecución                     │
│     • Varía según tipo de carga                 │
│                                                  │
│  2️⃣ Azure Compute (VMs)                         │
│     • Costo de las máquinas virtuales           │
│     • Basado en tipo y cantidad de nodos        │
│     • Puede usar spot instances (ahorro 80%)    │
│                                                  │
│  3️⃣ Storage                                     │
│     • Azure Data Lake Storage                   │
│     • Minimal comparado con compute             │
│                                                  │
│  4️⃣ Networking                                  │
│     • Transferencia de datos                    │
│     • Usualmente mínimo                         │
│                                                  │
└──────────────────────────────────────────────────┘
```

### Qué es un DBU (Databricks Unit)

Un **DBU** es una unidad de procesamiento normalizada por Databricks que representa:
- 1 DBU = capacidad de procesamiento por hora
- El precio de DBU varía según la carga de trabajo y región

### Tabla de Precios Estimados (Ejemplo - verificar precios actuales)

| Tipo de Carga | DBU/hora | VM (Standard_DS3_v2) | Total/hora/nodo |
|---------------|----------|----------------------|-----------------|
| **All-Purpose** | $0.55 | $0.15 | ~$0.70 |
| **Jobs (Batch)** | $0.22 | $0.15 | ~$0.37 |
| **Jobs Light** | $0.10 | $0.10 | ~$0.20 |
| **SQL Compute** | $0.22 | $0.15 | ~$0.37 |

**Ejemplo de cálculo**:
```
Cluster All-Purpose con 5 workers:
- Driver: 1 × $0.70/hora = $0.70/hora
- Workers: 5 × $0.70/hora = $3.50/hora
- TOTAL: $4.20/hora

Si el cluster corre 8 horas/día × 22 días/mes:
= 8 × 22 × $4.20 = $739.20/mes

Mismo cluster como Job Cluster:
= 8 × 22 × $2.22 = $391.68/mes
💰 AHORRO: $347.52/mes (47%)
```

---

## Tipos de Clusters y su Impacto en Costos

### All-Purpose vs Job Clusters

```
┌──────────────────────────────────────────────────────────┐
│                 COMPARACIÓN DE CLUSTERS                  │
├───────────────────┬──────────────────────────────────────┤
│                   │                                      │
│  ALL-PURPOSE      │        JOB CLUSTER                   │
│  💰💰💰           │        💰💰                          │
│                   │                                      │
│  ✅ Interactivo   │        ⚙️ Automatizado               │
│  ✅ Multi-usuario │        🔒 Single-purpose             │
│  ✅ Compartido    │        ⏱️ Efímero                    │
│  ❌ Más caro      │        ✅ 50% más barato             │
│  ❌ Puede quedar  │        ✅ Auto-terminación           │
│      encendido    │        ✅ Optimizado para costo      │
│                   │                                      │
│  USAR PARA:       │        USAR PARA:                    │
│  - Desarrollo     │        - Producción                  │
│  - Análisis       │        - ETL automatizado            │
│  - Exploración    │        - Jobs programados            │
│  - Prototipado    │        - Pipelines CI/CD             │
│                   │                                      │
└───────────────────┴──────────────────────────────────────┘
```

### Single Node vs Multi-Node

```python
# SINGLE NODE
# Costo: 1 VM (más barato)
# Uso: Desarrollo, testing, cargas pequeñas
{
    "cluster_name": "dev-single-node",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 0,  # ← Sin workers
    "spark_conf": {
        "spark.master": "local[*]"
    }
}

# Costo: ~$0.70/hora

# MULTI-NODE
# Costo: 1 driver + N workers
# Uso: Producción, datos grandes, procesamiento distribuido
{
    "cluster_name": "prod-multi-node",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 5
}

# Costo: ~$4.20/hora
```

### Tamaños de VM y su Impacto

| VM Type | vCPUs | RAM | Precio/hora | Uso Recomendado |
|---------|-------|-----|-------------|-----------------|
| **Standard_DS3_v2** | 4 | 14 GB | ~$0.15 | General, dev/test |
| **Standard_DS4_v2** | 8 | 28 GB | ~$0.30 | Workloads medianos |
| **Standard_E8_v3** | 8 | 64 GB | ~$0.40 | Memory-intensive |
| **Standard_F4s_v2** | 4 | 8 GB | ~$0.10 | Compute-intensive |
| **Standard_L8s_v2** | 8 | 64 GB | ~$0.50 | IO-intensive, storage |

```python
# Elegir el tipo de VM correcto puede ahorrar significativamente

# ❌ INCORRECTO: VM muy grande para datos pequeños
{
    "node_type_id": "Standard_E32_v3",  # 32 vCPUs, 256 GB RAM
    "num_workers": 10
}
# Costo: ~$16/hora para procesar 100 MB de datos

# ✅ CORRECTO: VM apropiada para la carga
{
    "node_type_id": "Standard_DS3_v2",  # 4 vCPUs, 14 GB RAM
    "num_workers": 2
}
# Costo: ~$2.10/hora para procesar 100 MB de datos
# 💰 AHORRO: 87%
```

---

## Estrategias de Optimización de Costos

### 1. Autoscaling

```python
# Cluster que escala dinámicamente
{
    "cluster_name": "autoscaling-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "autoscale": {
        "min_workers": 2,    # Baseline para disponibilidad
        "max_workers": 10    # Pico para demanda
    },
    "autotermination_minutes": 30
}

# Beneficios:
# ✅ Escala UP cuando hay carga
# ✅ Escala DOWN cuando no hay trabajo
# ✅ Paga solo por lo que usa
# ✅ Ahorro promedio: 40-60%
```

**Cómo funciona el autoscaling**:

```
Tiempo   │ Workers │ Costo/hora
─────────┼─────────┼───────────
08:00    │    2    │  $1.48
09:00    │    5    │  $3.70     ← Pico de trabajo
10:00    │    8    │  $5.92     ← Más carga
11:00    │    3    │  $2.22     ← Baja la carga
12:00    │    2    │  $1.48

Sin autoscaling (10 workers fijos):
Costo: 5 horas × $7.40 = $37.00

Con autoscaling (2-10 workers):
Costo: $1.48 + $3.70 + $5.92 + $2.22 + $1.48 = $14.80

💰 AHORRO: $22.20 (60%)
```

### 2. Spot Instances (Azure Spot VMs)

```python
# Usar Spot VMs para ahorrar hasta 80%
{
    "cluster_name": "spot-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 5,
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "first_on_demand": 1,           # Driver siempre on-demand
        "spot_bid_max_price": -1        # -1 = pagar hasta on-demand
    }
}

# Costo comparativo:
# On-Demand: 6 nodos × $0.70/hora = $4.20/hora
# Spot Mix:  1 on-demand + 5 spot = $0.70 + (5 × $0.14) = $1.40/hora
# 💰 AHORRO: $2.80/hora (67%)
```

**⚠️ Precauciones con Spot Instances**:
- Pueden ser revocadas por Azure si necesita capacidad
- **USAR PARA**: Jobs que pueden reiniciarse, análisis exploratorios
- **NO USAR PARA**: Jobs críticos sin tolerancia a fallos, streaming continuo

### 3. Auto-termination

```python
# Apagar clusters automáticamente cuando están idle
{
    "autotermination_minutes": 30  # Termina después de 30 min sin uso
}

# Caso de uso común:
# Desarrollador ejecuta notebook a las 10:00 AM
# Termina de trabajar a las 10:30 AM pero olvida apagar cluster
# 
# Sin auto-termination:
# Cluster corre hasta el fin del día (8 horas)
# Costo desperdiciado: 7.5 horas × $4.20 = $31.50
#
# Con auto-termination (30 min):
# Cluster se apaga a las 11:00 AM
# Costo real: 1 hora × $4.20 = $4.20
# 💰 AHORRO: $27.30
```

### 4. Cluster Pools

```python
# Cluster Pools: mantener VMs en "standby"
# Beneficio: startup 4x más rápido (15s vs 4-5 min)
# Costo: Solo storage del disco, no DBUs ni compute

# Crear un pool
{
    "instance_pool_name": "shared-pool",
    "node_type_id": "Standard_DS3_v2",
    "idle_instance_autotermination_minutes": 60,
    "min_idle_instances": 0,
    "max_capacity": 10
}

# Crear cluster desde el pool
{
    "cluster_name": "fast-start-cluster",
    "spark_version": "13.3.x-scala2.12",
    "instance_pool_id": "pool-abc123",
    "num_workers": 3
}

# ROI:
# Si ejecutas 50 jobs/día con 4 min de startup cada uno:
# Tiempo ahorrado: 50 × 3.5 min = 175 min/día
# En términos de costo: depende del valor del tiempo de espera
```

### 5. Cluster Policies (Governance)

```json
// Política para limitar costos
{
  "name": "Cost-Controlled Policy",
  "definition": {
    "cluster_type": {
      "type": "fixed",
      "value": "job"  // Forzar job clusters (más baratos)
    },
    "node_type_id": {
      "type": "allowlist",
      "values": [
        "Standard_DS3_v2",  // Solo VMs pequeñas/medianas
        "Standard_DS4_v2"
      ]
    },
    "autoscale.max_workers": {
      "type": "range",
      "maxValue": 10  // Limitar tamaño máximo
    },
    "autotermination_minutes": {
      "type": "range",
      "minValue": 10,
      "maxValue": 60
    },
    "azure_attributes.availability": {
      "type": "fixed",
      "value": "SPOT_AZURE"  // Forzar spot instances
    }
  }
}
```

---

## Configuraciones para Control de Costos

### Optimizaciones de Spark

```python
# Configuraciones que reducen el uso de recursos
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Estas configs:
# ✅ Reducen archivos pequeños
# ✅ Mejoran rendimiento de lecturas futuras
# ✅ Reducen tiempo de ejecución (= menos costo)
```

### Configuración Óptima por Tipo de Trabajo

```python
# DESARROLLO / EXPLORACIÓN
{
    "cluster_type": "all-purpose",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 0,  # Single node
    "autotermination_minutes": 20,
    "spark_conf": {
        "spark.master": "local[*]"
    }
}
# Costo: ~$0.70/hora

# ETL LIGERO (< 100 GB)
{
    "cluster_type": "job",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "first_on_demand": 1
    }
}
# Costo: ~$0.84/hora

# ETL PESADO (> 1 TB)
{
    "cluster_type": "job",
    "node_type_id": "Standard_DS4_v2",
    "autoscale": {
        "min_workers": 3,
        "max_workers": 10
    },
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "first_on_demand": 2
    }
}
# Costo: ~$2-8/hora (dinámico)

# MACHINE LEARNING
{
    "cluster_type": "all-purpose",
    "node_type_id": "Standard_NC6s_v3",  # GPU
    "num_workers": 2,
    "autotermination_minutes": 30
}
# Costo: ~$4-5/hora (justificado por GPU)
```

---

## Monitoreo de Costos

### Dashboard de Costos en Databricks

```sql
-- Query para analizar uso y costos
-- Disponible en el Account Console

SELECT 
    workspace_name,
    cluster_name,
    usage_date,
    sku_name,
    usage_quantity as DBUs_consumed,
    list_price * usage_quantity as estimated_cost
FROM system.billing.usage
WHERE usage_date >= date_sub(current_date(), 30)
ORDER BY estimated_cost DESC
LIMIT 50;
```

### Métricas Clave para Monitorear

```
┌─────────────────────────────────────────────────┐
│         MÉTRICAS DE COSTO CLAVE                 │
├─────────────────────────────────────────────────┤
│                                                 │
│  1. DBU consumption por workspace              │
│     → Identificar workspaces caros             │
│                                                 │
│  2. Cluster utilization (%)                    │
│     → Detectar clusters subutilizados          │
│                                                 │
│  3. All-Purpose vs Job cluster ratio           │
│     → Optimizar hacia Job clusters             │
│                                                 │
│  4. Average job duration                       │
│     → Optimizar queries lentas                 │
│                                                 │
│  5. Spot instance success rate                 │
│     → Ajustar configuración de spot            │
│                                                 │
│  6. Idle time antes de auto-termination        │
│     → Reducir tiempo de idle                   │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Alertas de Costos

```python
# Script para monitorear y alertar sobre costos altos
from datetime import datetime, timedelta
import requests

def check_daily_cost():
    # Obtener uso del día actual
    today = datetime.now().date()
    
    usage_query = f"""
    SELECT SUM(list_price * usage_quantity) as daily_cost
    FROM system.billing.usage
    WHERE usage_date = '{today}'
    """
    
    result = spark.sql(usage_query).collect()
    daily_cost = result[0]['daily_cost']
    
    # Umbral de alerta: $500/día
    ALERT_THRESHOLD = 500
    
    if daily_cost > ALERT_THRESHOLD:
        send_alert(
            subject=f"⚠️ Azure Databricks: Costo Diario Alto",
            message=f"Costo actual del día: ${daily_cost:.2f}\n"
                    f"Umbral: ${ALERT_THRESHOLD}\n"
                    f"Excedente: ${daily_cost - ALERT_THRESHOLD:.2f}"
        )
        
        # Identificar los clusters más costosos
        top_clusters_query = f"""
        SELECT 
            cluster_name,
            SUM(usage_quantity) as dbus,
            SUM(list_price * usage_quantity) as cost
        FROM system.billing.usage
        WHERE usage_date = '{today}'
        GROUP BY cluster_name
        ORDER BY cost DESC
        LIMIT 5
        """
        
        top_clusters = spark.sql(top_clusters_query).toPandas()
        print("Top 5 clusters más costosos hoy:")
        print(top_clusters)

# Ejecutar en un job programado cada hora
check_daily_cost()
```

---

## Mejores Prácticas

### ✅ DO's (Hacer)

```python
# 1. Usar Job Clusters para producción
{
    "cluster_type": "job",
    "azure_attributes": {"availability": "SPOT_AZURE"}
}

# 2. Configurar auto-termination
{
    "autotermination_minutes": 30
}

# 3. Usar autoscaling
{
    "autoscale": {"min_workers": 2, "max_workers": 10}
}

# 4. Cachear DataFrames que se reusan
df_cached = df.cache()
df_cached.count()  # Materializar cache

# 5. Usar Delta Lake con optimizaciones
df.write.format("delta") \
    .option("optimizeWrite", "true") \
    .save("/path")

# 6. Particionar datos apropiadamente
df.write.partitionBy("year", "month").save("/path")

# 7. Usar Broadcast Joins para tablas pequeñas
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), "key")

# 8. Limitar resultados en desarrollo
df.limit(10000).display()  # No cargar todo en UI
```

### ❌ DON'Ts (No Hacer)

```python
# 1. ❌ Dejar clusters All-Purpose corriendo 24/7
# Costo: $4.20/hora × 24 × 30 = $3,024/mes
# Solución: Auto-termination o apagar manualmente

# 2. ❌ Usar clusters muy grandes para datos pequeños
# Costo innecesario: 20 workers para 1 GB de datos

# 3. ❌ No cachear datos que se reusan
# Recalcula los mismos datos múltiples veces

# 4. ❌ Collect() en datasets grandes
df.collect()  # ❌ Trae TODO a driver (puede crashear y desperdiciar recursos)
df.limit(100).collect()  # ✅ Mejor

# 5. ❌ Crear muchos archivos pequeños
# Aumenta tiempo de lectura y costos de storage operations

# 6. ❌ Usar All-Purpose clusters para Jobs automatizados
# Paga 2x más DBUs innecesariamente

# 7. ❌ No monitorear costos regularmente
# No puedes optimizar lo que no mides
```

---

## 💰 Calculadora de Ahorros

### Escenario: Optimización de una Solución Típica

**ANTES (Sin optimización)**:
```
Cluster All-Purpose:
- 10 workers Standard_DS4_v2 
- Corriendo 12 horas/día
- 22 días/mes
- $0.60/worker/hora

Costo mensual: 10 × $0.60 × 12 × 22 = $1,584/mes
```

**DESPUÉS (Optimizado)**:
```
Job Cluster con optimizaciones:
- 2-8 workers (autoscaling) Standard_DS3_v2
- Spot instances
- Solo corre cuando hay jobs (~8 horas/día)
- Auto-termination configurado
- $0.21/worker/hora (promedio con spot)

Costo mensual: 5 (promedio) × $0.21 × 8 × 22 = $184.80/mes

💰 AHORRO: $1,399.20/mes (88%)
💰 AHORRO ANUAL: $16,790.40
```

---

## 🎯 Checklist de Optimización de Costos

Al configurar un cluster, verificar:

- [ ] **Tipo de cluster**: Job cluster para producción
- [ ] **Autoscaling**: Habilitado con límites apropiados
- [ ] **Auto-termination**: Configurado (20-60 minutos)
- [ ] **Spot instances**: Habilitado para workers
- [ ] **Tamaño de VM**: Apropiado para la carga de trabajo
- [ ] **Cluster Policy**: Aplicada para governanza
- [ ] **Monitoreo**: Dashboard de costos revisado semanalmente
- [ ] **Spark configs**: Optimizaciones habilitadas
- [ ] **Particionamiento**: Datos particionados apropiadamente
- [ ] **Delta optimizations**: Optimize y Z-ordering configurados

---

## 📚 Preguntas para el Partner sobre Costos

Al hacer Knowledge Transfer con el partner, preguntar:

1. ¿Cuál es el presupuesto mensual esperado de Databricks?
2. ¿Qué clusters están corriendo actualmente y para qué?
3. ¿Hay clusters All-Purpose que podrían convertirse a Job clusters?
4. ¿Se están usando spot instances? ¿Por qué sí o no?
5. ¿Cómo se monitorean los costos actualmente?
6. ¿Hay alertas configuradas para costos excesivos?
7. ¿Cuál es el SLA de los jobs? (afecta tolerancia a spot revocations)
8. ¿Se han optimizado las queries Spark?
9. ¿Se usa Delta Lake con optimizaciones habilitadas?
10. ¿Quién tiene permisos para crear clusters? (governance)

---

**Anterior**: [03 - Arquitectura e Integración](./03-arquitectura-integracion.md)  
**Siguiente**: [05 - Notebooks y Jobs](./05-notebooks-jobs.md)
