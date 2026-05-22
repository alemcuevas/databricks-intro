# Laboratorio 2: Configuración y Gestión de Clusters

⏱️ **Duración estimada**: 30-40 minutos  
🎯 **Nivel**: Principiante-Intermedio

## 📋 Objetivos

Al final de este laboratorio, serás capaz de:
- Crear y configurar diferentes tipos de clusters
- Entender la diferencia entre All-Purpose y Job clusters
- Configurar autoscaling, Spot instances y auto-termination
- Crear una Cluster Policy para governance
- Monitorear uso y costos de clusters

---

## Ejercicio 1: Crear All-Purpose Cluster (10 min)

### Paso 1: Crear Cluster desde la UI

1. **Ir a Compute**:
   - En el menú lateral, click en "Compute"
   - Click en "Create Cluster"

2. **Configuración básica**:
   ```
   Cluster name: lab-cluster-dev
   Cluster mode: Standard
   Databricks Runtime Version: 13.3 LTS (Scala 2.12, Spark 3.4.1)
   Autopilot: Enable
   ```

3. **Configurar workers**:
   ```
   Worker type: Standard_DS3_v2
   Min workers: 2
   Max workers: 4
   Enable autoscaling: ✅
   ```

4. **Advanced Options**:
   ```
   Auto Termination: 30 minutes
   Logging:
     - Destination: DBFS
     - Path: dbfs:/cluster-logs/lab-cluster-dev
   Tags:
     - Project: Training
     - Environment: Development
   ```

5. **Click "Create Cluster"**
   - Espera ~3-5 minutos mientras inicia

### Paso 2: Verificar Cluster Info

Una vez iniciado, verifica:

```
✅ Estado: Running
✅ Workers activos: 2 (puede escalar hasta 4)
✅ Memory: ~14 GB por worker
✅ Cores: 4 cores por worker
```

**Calcular costo estimado**:
```
4 cores × 2 workers = 8 cores total
DBUs = 8 cores × 0.75 DBU/core = 6 DBUs/hora
Costo DBU (Premium All-Purpose): 6 × $0.55 = $3.30/hora
Costo VM: 2 workers × $0.15/hora = $0.30/hora
TOTAL: $3.60/hora

Si corre 8 horas/día × 30 días = $864/mes ⚠️
```

---

## Ejercicio 2: Probar Autoscaling (10 min)

### Paso 1: Crear Notebook de Prueba

1. Crea un nuevo notebook: "Test_Autoscaling"
2. Adjúntalo al cluster "lab-cluster-dev"

### Paso 2: Generar Carga Ligera

```python
# Databricks notebook source
# MAGIC %md
# MAGIC ## Test de Autoscaling

# COMMAND ----------

# Carga ligera: 2 workers pueden manejar esto
from pyspark.sql.functions import rand

df_small = spark.range(0, 1000000) \
    .withColumn("value", rand() * 100)

print(f"Registros: {df_small.count():,}")

# COMMAND ----------

# Ver workers activos
sc = spark.sparkContext
print(f"Workers activos: {len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1}")  # -1 para excluir driver
```

**Resultado esperado**: 2 workers (carga baja)

### Paso 3: Generar Carga Alta

```python
# COMMAND ----------

# Carga alta: forzar scale-up a 4 workers
df_large = spark.range(0, 50000000) \
    .repartition(16) \
    .withColumn("value1", rand() * 100) \
    .withColumn("value2", rand() * 100) \
    .withColumn("value3", rand() * 100)

# Operación costosa
df_agg = df_large.groupBy("value1").agg(
    {"value2": "sum", "value3": "avg"}
)

print(f"Grupos: {df_agg.count():,}")

# COMMAND ----------

# Verificar workers después de carga
import time
time.sleep(30)  # Esperar a que autoscaling actúe

print(f"Workers activos: {len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1}")
```

**Resultado esperado**: 3-4 workers (escala automáticamente)

### Paso 4: Monitorear en Spark UI

1. En el notebook, click en "Cluster: lab-cluster-dev"
2. Tab "Spark UI"
3. Ver "Executors" → Deberías ver más executors activos
4. Ver "Stages" → Ver distribución de tasks

---

## Ejercicio 3: Configurar Spot Instances (5 min)

### Paso 1: Crear Cluster con Spot

1. Compute → Create Cluster
2. **Configuración**:
   ```
   Cluster name: lab-cluster-spot
   Runtime: 13.3 LTS
   Worker type: Standard_DS3_v2
   Min workers: 2
   Max workers: 4
   ```

3. **Advanced Options → Azure**:
   ```
   Spot Instances:
     - First on Demand: 1
     - Availability: Spot with fallback
     - Max price: -1 (sin límite, acepta precio de mercado)
   ```

4. Crear y esperar a que inicie

### Paso 2: Verificar Configuración

En el cluster, ir a "Configuration" (JSON) y verificar:

```json
{
  "azure_attributes": {
    "availability": "SPOT_WITH_FALLBACK_AZURE",
    "first_on_demand": 1,
    "spot_bid_max_price": -1
  }
}
```

**Interpretación**:
- ✅ Driver siempre On-Demand (reliability)
- ✅ Worker 1: On-Demand (garantizado)
- ✅ Workers 2-4: Spot (hasta 80% ahorro)

**Ahorro estimado**:
```
Sin Spot: 4 workers × $0.15/hora = $0.60/hora
Con Spot: 1 driver + 1 worker On-Demand ($0.30) + 2 Spot workers ($0.06)
         = $0.36/hora
Ahorro: 40% ($0.24/hora)
```

---

## Ejercicio 4: Crear Job Cluster Config (5 min)

Los Job Clusters no se crean directamente, sino que se definen en Jobs.  
Vamos a preparar la configuración.

### Paso 1: Definir Configuración Óptima

Crea un archivo de texto con esta config (para usar luego en Jobs):

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
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
  },
  "azure_attributes": {
    "availability": "SPOT_WITH_FALLBACK_AZURE",
    "first_on_demand": 1,
    "spot_bid_max_price": -1
  },
  "enable_elastic_disk": true,
  "autotermination_minutes": 10,
  "custom_tags": {
    "Project": "ETL",
    "Environment": "Production",
    "CostCenter": "DataEngineering"
  }
}
```

**Características clave**:
- ✅ Autoscaling 2-8 (flexible)
- ✅ Spot instances (ahorro)
- ✅ Delta Lake optimizations (performance)
- ✅ Auto-termination 10 min (jobs terminan rápido)
- ✅ Tags para cost tracking

---

## Ejercicio 5: Crear Cluster Policy (10 min)

Las Cluster Policies permiten a administradores controlar qué pueden crear los usuarios.

### Paso 1: Crear Policy (Admin Settings)

1. **Settings (⚙️) → Compute → Cluster Policies**
2. **Create Policy**
3. **Nombre**: "Development Standard"
4. **Policy Definition (JSON)**:

```json
{
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2"
    ],
    "defaultValue": "Standard_DS3_v2"
  },
  "spark_version": {
    "type": "regex",
    "pattern": "^13\\.3\\..*-scala.*$"
  },
  "autoscale.max_workers": {
    "type": "range",
    "maxValue": 6
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 120,
    "defaultValue": 30
  },
  "spark_conf.spark.databricks.cluster.profile": {
    "type": "fixed",
    "value": "singleNode",
    "hidden": false
  },
  "custom_tags.CostCenter": {
    "type": "fixed",
    "value": "Engineering"
  }
}
```

5. **Guardar**

### Paso 2: Probar la Policy

1. **Crear nuevo cluster**
2. **Policy**: Selecciona "Development Standard"
3. Observa que:
   - ✅ Solo VM types permitidos aparecen
   - ✅ Runtime version filtrado a 13.3.x
   - ✅ Max workers limitado a 6
   - ✅ Auto-termination pre-configurado
   - ✅ Tag "CostCenter" automático

---

## Ejercicio 6: Monitorear Costos (5 min)

### Paso 1: Ver Uso de Cluster

1. **Compute → [tu cluster]**
2. **Metrics**:
   - Uptime
   - Memory usage
   - CPU usage
   - DBU consumption

### Paso 2: Calcular Costos

```python
# Databricks notebook source
# MAGIC %md
# MAGIC ## Calculadora de Costos de Cluster

# COMMAND ----------

def calculate_cluster_cost(
    num_workers,
    worker_cores,
    hours_per_day,
    days_per_month,
    cluster_type="all_purpose",
    spot_percentage=0
):
    """
    Calcula costo mensual de un cluster
    """
    # DBUs
    dbus_per_worker = worker_cores * 0.75
    total_dbus_per_hour = dbus_per_worker * num_workers
    
    # Precio DBU
    dbu_price = 0.55 if cluster_type == "all_purpose" else 0.22
    
    # VM cost (ejemplo: DS3_v2 = $0.15/hora)
    vm_cost_per_worker = 0.15
    vm_cost_per_hour = vm_cost_per_worker * num_workers
    
    # Ajustar por Spot
    if spot_percentage > 0:
        spot_workers = int(num_workers * spot_percentage / 100)
        on_demand_workers = num_workers - spot_workers
        vm_cost_per_hour = (on_demand_workers * vm_cost_per_worker) + (spot_workers * vm_cost_per_worker * 0.2)
    
    # Total
    hours_per_month = hours_per_day * days_per_month
    dbu_cost = total_dbus_per_hour * dbu_price * hours_per_month
    vm_cost = vm_cost_per_hour * hours_per_month
    
    total_cost = dbu_cost + vm_cost
    
    print("=" * 50)
    print(f"Configuración: {num_workers} workers × {worker_cores} cores")
    print(f"Uso: {hours_per_day} hrs/día × {days_per_month} días")
    print(f"Tipo: {cluster_type.title()}")
    if spot_percentage > 0:
        print(f"Spot: {spot_percentage}% workers")
    print("=" * 50)
    print(f"DBUs/hora: {total_dbus_per_hour:.2f}")
    print(f"Costo DBU: ${dbu_cost:,.2f}/mes")
    print(f"Costo VM: ${vm_cost:,.2f}/mes")
    print(f"TOTAL: ${total_cost:,.2f}/mes")
    print("=" * 50)
    
    return total_cost

# COMMAND ----------

# Ejemplo 1: All-Purpose cluster sin optimizaciones
print("\n📊 Escenario 1: All-Purpose, Sin Optimizaciones")
cost1 = calculate_cluster_cost(
    num_workers=4,
    worker_cores=4,
    hours_per_day=8,
    days_per_month=22,
    cluster_type="all_purpose",
    spot_percentage=0
)

# COMMAND ----------

# Ejemplo 2: All-Purpose con Spot
print("\n📊 Escenario 2: All-Purpose, 75% Spot")
cost2 = calculate_cluster_cost(
    num_workers=4,
    worker_cores=4,
    hours_per_day=8,
    days_per_month=22,
    cluster_type="all_purpose",
    spot_percentage=75
)

print(f"\n💰 Ahorro: ${cost1 - cost2:,.2f}/mes ({(1 - cost2/cost1)*100:.1f}%)")

# COMMAND ----------

# Ejemplo 3: Job cluster con Spot
print("\n📊 Escenario 3: Job Cluster, 75% Spot")
cost3 = calculate_cluster_cost(
    num_workers=4,
    worker_cores=4,
    hours_per_day=2,  # Jobs corren menos tiempo
    days_per_month=30,
    cluster_type="job",
    spot_percentage=75
)

print(f"\n💰 Ahorro vs Escenario 1: ${cost1 - cost3:,.2f}/mes ({(1 - cost3/cost1)*100:.1f}%)")
```

---

## 🎯 Desafío Final

Crea la configuración óptima para estos escenarios:

### Escenario A: Desarrollo Interactivo
**Requisitos**:
- 3 data scientists trabajando simultáneamente
- Exploración de datasets (~100 GB)
- Uso típico: 6 horas/día

**Tu solución**:
- Tipo de cluster: ________________
- Workers: min _____, max _____
- VM type: ________________
- Spot: Sí ☐ No ☐
- Auto-termination: _____ minutos
- Costo estimado: $______/mes

### Escenario B: ETL Nocturno
**Requisitos**:
- Job que corre 1:00 AM - 3:00 AM diariamente
- Procesa ~500 GB/día
- Crítico para reportes matutinos

**Tu solución**:
- Tipo de cluster: ________________
- Workers: min _____, max _____
- VM type: ________________
- Spot: Sí ☐ No ☐  (¿Por qué?)
- Retry: Sí ☐ No ☐
- Costo estimado: $______/mes

---

## ✅ Checklist de Completado

- ☐ Creado All-Purpose cluster con autoscaling
- ☐ Probado autoscaling con carga variable
- ☐ Configurado Spot instances
- ☐ Preparado Job cluster config
- ☐ Creado Cluster Policy
- ☐ Calculado costos de diferentes configuraciones
- ☐ Completado desafío final

---

## 📚 Recursos Adicionales

- [Cluster Configuration Best Practices](https://docs.databricks.com/clusters/configure.html)
- [Azure Spot VMs for Databricks](https://docs.microsoft.com/azure/databricks/clusters/azure-spot)
- [Cost Optimization Guide](https://docs.databricks.com/administration-guide/cost-analysis.html)

---

**Siguiente**: [Laboratorio 3 - Notebooks Avanzados](./lab-03-notebooks.md)
