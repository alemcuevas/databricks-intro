# 📘 Documentación Detallada del Código - Notebooks

Este documento proporciona explicaciones detalladas y comentarios sobre todo el código presente en los notebooks del curso de Azure Databricks.

---

## 📓 Lab 01 - Configuración Inicial y Workspace

### Parte 1: Crear DataFrame Simple

#### Importaciones
```python
from pyspark.sql.functions import col, upper, when, current_timestamp
```

**Explicación:**
- `col`: Función para referenciar columnas de un DataFrame de forma explícita
- `upper`: Convierte texto a mayúsculas
- `when`: Implementa lógica condicional (equivalente a IF-THEN-ELSE)
- `current_timestamp`: Retorna el timestamp actual del sistema

#### Creación de DataFrame
```python
data = [
    (1, "Alice", "Ventas", 75000),
    (2, "Bob", "IT", 85000),
    (3, "Charlie", "Ventas", 65000),
    (4, "Diana", "Marketing", 70000),
    (5, "Eve", "IT", 90000)
]
columns = ["id", "nombre", "departamento", "salario"]
df = spark.createDataFrame(data, columns)
```

**Explicación detallada:**
1. **data**: Lista de tuplas donde cada tupla representa un registro (fila)
2. **columns**: Lista de strings con los nombres de las columnas
3. **spark.createDataFrame()**: 
   - Método principal para crear DataFrames en PySpark
   - `spark` es la SparkSession disponible automáticamente en notebooks de Databricks
   - Infiere automáticamente los tipos de datos basándose en los valores
   - En este caso: (int, string, string, int)

**Conceptos importantes:**
- **DataFrame**: Colección distribuida e inmutable de datos organizados en columnas con nombre
- **Lazy Evaluation**: Las transformaciones se definen pero no ejecutan hasta que se llama una acción
- **Schema**: Estructura de datos (nombres y tipos de columnas)

#### Visualización
```python
print(f"✅ DataFrame creado con {df.count()} registros")
display(df)
```

**Explicación:**
- `df.count()`: **ACCIÓN** que dispara la ejecución del plan de Spark y retorna el número de filas
- `display()`: Función especial de Databricks para visualización interactiva con opciones de gráficos

**Acciones vs Transformaciones:**
- **Transformaciones** (lazy): `select()`, `filter()`, `withColumn()` - No ejecutan inmediatamente
- **Acciones** (eager): `count()`, `collect()`, `show()` - Disparan la ejecución

---

### Parte 2: Transformaciones Básicas

```python
df_transformed = df \
    .withColumn("nombre_upper", upper(col("nombre"))) \
    .withColumn("nivel_salarial",
                when(col("salario") < 70000, "Junior")
                .when(col("salario") < 80000, "Mid")
                .otherwise("Senior")) \
    .withColumn("fecha_proceso", current_timestamp())
```

**Explicación línea por línea:**

1. **`.withColumn("nombre_upper", upper(col("nombre")))`**
   - Crea nueva columna "nombre_upper"
   - Aplica `upper()` a la columna "nombre"
   - El DataFrame original NO se modifica (inmutabilidad)
   - Retorna un NUEVO DataFrame con la columna adicional

2. **`.withColumn("nivel_salarial", when(...).otherwise(...))`**
   - Implementa lógica condicional en cadena
   - `when(condicion, valor)`: Si la condición es verdadera, asigna el valor
   - `.when(otra_condicion, otro_valor)`: Evalúa siguiente condición si la anterior fue falsa
   - `.otherwise(valor_default)`: Valor por defecto si ninguna condición se cumple
   - Similar a SQL CASE WHEN

3. **`.withColumn("fecha_proceso", current_timestamp())`**
   - Agrega columna con timestamp de ejecución
   - Útil para tracking de procesamiento y auditoría

**Operador backslash (\\):**
- Permite dividir comandos largos en múltiples líneas
- Mejora legibilidad sin afectar funcionalidad
- Python interpreta las líneas como una sola expresión

---

### Parte 3: Filtros y Agregaciones

#### Filtrado
```python
df_it = df_transformed.filter(col("departamento") == "IT")
print(f"Empleados de IT: {df_it.count()}")
```

**Explicación:**
- `filter()` o `where()` (sinónimos): Aplican predicado para seleccionar registros
- `col("departamento") == "IT"`: Expresión booleana que retorna True/False por fila
- Equivalente SQL: `WHERE departamento = 'IT'`
- Solo las filas que cumplen la condición se incluyen en el resultado

**Alternativas de sintaxis:**
```python
# Opción 1: Usando col()
df.filter(col("departamento") == "IT")

# Opción 2: String SQL (menos seguro)
df.filter("departamento = 'IT'")

# Opción 3: Atributo (solo para columnas sin espacios)
df.filter(df.departamento == "IT")
```

#### Agregaciones
```python
from pyspark.sql.functions import avg, count, sum as _sum

df_summary = df.groupBy("departamento").agg(
    count("*").alias("num_empleados"),
    avg("salario").alias("salario_promedio"),
    _sum("salario").alias("total_salarios")
).orderBy("salario_promedio", ascending=False)
```

**Explicación detallada:**

1. **Importación `sum as _sum`:**
   - Evita conflicto con `sum()` built-in de Python
   - Convención: Prefijo `_` para funciones de PySpark

2. **`groupBy("departamento")`:**
   - Agrupa registros por los valores únicos de "departamento"
   - Similar a SQL: `GROUP BY departamento`

3. **`.agg()`:**
   - Aplica funciones de agregación a cada grupo
   - Acepta múltiples funciones simultáneamente

4. **Funciones de agregación:**
   - `count("*")`: Cuenta registros en cada grupo
   - `avg("salario")`: Calcula promedio de columna "salario"
   - `_sum("salario")`: Suma todos los valores de "salario"

5. **`.alias("nombre")`:**
   - Renombra la columna resultante
   - Sin alias, nombres serían: "count(1)", "avg(salario)", etc.

6. **`.orderBy("salario_promedio", ascending=False)`:**
   - Ordena resultados por la columna especificada
   - `ascending=False`: Orden descendente (mayor a menor)
   - `ascending=True` (default): Orden ascendente

**Otras funciones de agregación útiles:**
- `min()`, `max()`: Valores mínimo y máximo
- `stddev()`: Desviación estándar
- `countDistinct()`: Cuenta valores únicos
- `collect_list()`: Agrupa valores en una lista
- `first()`, `last()`: Primer/último valor

---

### Parte 4: Guardar Datos

```python
output_path = "/tmp/lab01/empleados"

df_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)
```

**Explicación completa:**

1. **Ruta DBFS:**
   - `/tmp/`: Directorio temporal en Databricks File System
   - Ideal para desarrollo y pruebas
   - Para producción: usar Azure Data Lake Storage (ADLS)

2. **`.format("delta")`:**
   - Especifica Delta Lake como formato de almacenamiento
   - **Delta Lake** proporciona:
     - ACID transactions (Atomicidad, Consistencia, Aislamiento, Durabilidad)
     - Time Travel (consultar versiones históricas)
     - Schema enforcement y evolution
     - Upserts y deletes eficientes
     - Mejor compresión y performance vs Parquet

3. **`.mode("overwrite")`:**
   - **overwrite**: Elimina datos existentes y escribe nuevos
   - **append**: Agrega nuevos datos sin eliminar existentes
   - **ignore**: No escribe si ya existen datos (falla silenciosamente)
   - **error/errorifexists** (default): Lanza excepción si existen datos

4. **`.save(path)`:**
   - Ejecuta la escritura (es una ACCIÓN)
   - Crea directorios automáticamente si no existen

**Formatos de almacenamiento en Databricks:**
| Formato | Uso | Pros | Contras |
|---------|-----|------|---------|
| Delta | Lakehouse (recomendado) | ACID, Time Travel, Performance | Específico de Databricks |
| Parquet | Almacenamiento columnar | Compresión, compatible | No tiene ACID |
| CSV | Intercambio con sistemas externos | Universal | Lento, sin tipos |
| JSON | APIs, datos semi-estructurados | Flexible | Verboso, lento |

---

### Parte 5: Leer Datos

```python
df_read = spark.read.format("delta").load(output_path)
print(f"📊 Registros leídos: {df_read.count()}")
```

**Explicación:**
- `spark.read`: Crea un DataFrameReader
- `.format("delta")`: Especifica formato a leer
- `.load(path)`: Lee datos desde la ruta especificada
- Retorna un DataFrame con los datos

**Lectura con opciones adicionales:**
```python
# Leer versión específica (Time Travel)
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(path)

# Leer en timestamp específico
df_ts = spark.read.format("delta").option("timestampAsOf", "2026-05-20").load(path)

# Leer con schema inferido
df = spark.read.format("parquet").option("inferSchema", "true").load(path)
```

---

### Parte 6: SQL Queries

```python
df_transformed.createOrReplaceTempView("empleados")
```

**Explicación:**
- **Temp View**: Vista temporal que mapea un DataFrame a una tabla SQL
- Existe solo durante la sesión de Spark actual
- Permite consultar DataFrames usando SQL estándar
- `createOrReplaceTempView()`: Crea o reemplaza si ya existe

**Alternativas:**
- `createTempView()`: Falla si ya existe (más seguro)
- `createGlobalTempView()`: Vista disponible en todas las sesiones (en namespace `global_temp`)
- `createOrReplaceGlobalTempView()`: Versión replace de la anterior

```sql
%%sql
SELECT 
    departamento,
    nivel_salarial,
    COUNT(*) as num_empleados,
    AVG(salario) as salario_promedio
FROM empleados
GROUP BY departamento, nivel_salarial
ORDER BY departamento, salario_promedio DESC
```

**Explicación:**
- `%%sql`: Magic command para ejecutar SQL en la celda
- Query SQL estándar que consulta la temp view "empleados"
- Resultados se muestran automáticamente en formato de tabla

**Ventajas de SQL en Databricks:**
- Sintaxis familiar para usuarios de SQL
- Mismo motor de ejecución (Spark SQL)
- Performance idéntica a API de DataFrames
- Facilita integración con herramientas de BI

---

## 📓 Lab 02 - Clusters y Optimización

### Ejercicio 1: Test de Autoscaling - Carga Ligera

```python
from pyspark.sql.functions import rand
import time

print("🔹 Carga Ligera: Procesando 1 millón de registros")
start = time.time()

df_light = spark.range(0, 1_000_000) \
    .withColumn("value", rand() * 1000) \
    .withColumn("category", (rand() * 10).cast("int"))

result = df_light.groupBy("category").count().collect()

end = time.time()
print(f"✅ Completado en {end - start:.2f} segundos")
```

**Explicación detallada:**

1. **`spark.range(0, 1_000_000)`:**
   - Genera DataFrame con columna "id" desde 0 hasta 999,999
   - Operación muy eficiente (no carga datos en memoria)
   - Útil para testing y generación de datos sintéticos

2. **`rand() * 1000`:**
   - `rand()`: Genera número aleatorio entre 0 y 1
   - Multiplica por 1000 para obtener rango [0, 1000]
   - Cada fila obtiene un valor diferente

3. **`.cast("int")`:**
   - Convierte tipo de dato a entero
   - Elimina decimales (truncamiento)
   - `(rand() * 10).cast("int")` genera enteros de 0 a 9

4. **`.collect()`:**
   - **ACCIÓN** que trae TODOS los resultados al driver
   - ⚠️ PRECAUCIÓN: Solo usar con datasets pequeños
   - Para datasets grandes: usar `.take(n)` o `.limit(n)`

5. **Medición de tiempo:**
   - `time.time()`: Retorna timestamp actual en segundos
   - `end - start`: Calcula duración de ejecución
   - Útil para benchmarking y optimización

**Propósito del ejercicio:**
- Carga ligera NO debería escalar a más workers
- Cluster mantiene configuración mínima
- Observar comportamiento en Spark UI

---

### Ejercicio 2: Test de Autoscaling - Carga Pesada

```python
print("🔸 Carga Pesada: Procesando 50 millones de registros")
start = time.time()

df_heavy = spark.range(0, 50_000_000) \
    .withColumn("value", rand() * 1000) \
    .withColumn("category", (rand() * 100).cast("int")) \
    .repartition(16)  # Forzar más paralelismo

from pyspark.sql.functions import avg, sum, stddev, min, max

result = df_heavy.groupBy("category").agg(
    avg("value").alias("avg_value"),
    sum("value").alias("sum_value"),
    stddev("value").alias("stddev_value"),
    min("value").alias("min_value"),
    max("value").alias("max_value")
).collect()
```

**Explicación detallada:**

1. **`.repartition(16)`:**
   - **Reparticionamiento**: Redistribuye datos en N particiones
   - Fuerza shuffle (movimiento de datos entre executors)
   - Incrementa paralelismo (más tareas concurrentes)
   - Útil cuando tienes muchos workers pero pocas particiones
   - **Trade-off**: Shuffle tiene costo, pero mejora paralelismo

**¿Cuándo reparticionar?**
- Datos muy desbalanceados entre particiones
- Pocos executors trabajando (otros ociosos)
- Antes de writes para controlar número de archivos
- Después de filtros agresivos que reducen datos drásticamente

2. **Agregaciones complejas:**
   - `avg()`: Promedio aritmético
   - `sum()`: Suma total
   - `stddev()`: Desviación estándar (medida de dispersión)
   - `min()`, `max()`: Valores extremos

3. **Propósito del ejercicio:**
   - Carga pesada DEBE activar autoscaling
   - Observar en Spark UI:
     - Número de workers incrementa
     - Tareas se distribuyen
     - Tiempo de shuffle
   - Cluster debe escalar de min_workers a max_workers

---

### Ejercicio 3: Calculadora de Costos

```python
def calcular_costo_cluster(vm_type, num_workers, horas_mes, 
                          tipo_cluster="all_purpose", usar_spot=False):
    """
    Calcula el costo mensual de un cluster de Databricks.
    
    Args:
        vm_type (str): Tipo de VM Azure (ej: "Standard_DS3_v2")
        num_workers (int): Número de workers en el cluster
        horas_mes (int): Horas de ejecución por mes
        tipo_cluster (str): "all_purpose" o "jobs"
        usar_spot (bool): True para usar Spot VMs (40% descuento)
    
    Returns:
        dict: Desglose de costos con métricas
    
    Ejemplo:
        >>> calcular_costo_cluster("Standard_DS3_v2", 3, 160, "jobs", True)
        {'costo_mensual': 285.12, 'ahorro_anual': 3421.44, ...}
    """
    
    # Configuración de precios (ejemplo para East US)
    vm_precios = {
        "Standard_DS3_v2": {"cores": 4, "ram_gb": 14, "precio_hora": 0.192},
        "Standard_DS4_v2": {"cores": 8, "ram_gb": 28, "precio_hora": 0.384},
        "Standard_DS5_v2": {"cores": 16, "ram_gb": 56, "precio_hora": 0.768},
        "Standard_E4s_v3": {"cores": 4, "ram_gb": 32, "precio_hora": 0.252}
    }
    
    # DBU pricing (Premium tier)
    # DBU = Databricks Unit = Medida de procesamiento
    dbu_precios = {
        "all_purpose": 0.55,  # USD por DBU (clusters interactivos)
        "jobs": 0.22          # USD por DBU (50% más barato para jobs automatizados)
    }
    
    # Obtener configuración de VM
    vm_config = vm_precios.get(vm_type, vm_precios["Standard_DS3_v2"])
    
    # Calcular DBUs por hora
    # Fórmula aproximada: cores × 0.75 × num_workers
    # Factor 0.75: Ajuste de Databricks basado en capacidad real
    dbus_por_hora = vm_config["cores"] * 0.75 * num_workers
    
    # Calcular costos por hora
    costo_vm_hora = vm_config["precio_hora"] * num_workers  # Costo de VMs Azure
    costo_dbu_hora = dbus_por_hora * dbu_precios[tipo_cluster]  # Costo de DBUs
    
    # Aplicar descuento spot si está habilitado
    if usar_spot:
        # Spot VMs: 40% descuento promedio (puede variar por región/disponibilidad)
        costo_vm_hora *= 0.60  # Pagar solo 60% del precio
    
    # Costo total por hora = VM + DBU
    costo_total_hora = costo_vm_hora + costo_dbu_hora
    
    # Costo mensual
    costo_mensual = costo_total_hora * horas_mes
    
    return {
        "vm_type": vm_type,
        "workers": num_workers,
        "cores_total": vm_config["cores"] * num_workers,
        "ram_total_gb": vm_config["ram_gb"] * num_workers,
        "dbus_hora": round(dbus_por_hora, 2),
        "costo_vm_hora": round(costo_vm_hora, 2),
        "costo_dbu_hora": round(costo_dbu_hora, 2),
        "costo_total_hora": round(costo_total_hora, 2),
        "horas_mes": horas_mes,
        "costo_mensual": round(costo_mensual, 2),
        "costo_anual": round(costo_mensual * 12, 2),
        "tipo_cluster": tipo_cluster,
        "usa_spot": usar_spot
    }
```

**Componentes del costo en Databricks:**

1. **Costo de VM (Infrastructure):**
   - Precio de las máquinas virtuales de Azure
   - Basado en: CPU, RAM, Storage
   - Variable por región y tipo de VM
   - Spot VMs: 40-80% más baratas (sin garantía de disponibilidad)

2. **Costo de DBU (Databricks Units):**
   - Unidad de medida de procesamiento de Databricks
   - Incluye: Software, gestión, optimizaciones, soporte
   - All-Purpose ($0.55/DBU): Clusters interactivos, desarrollo
   - Jobs ($0.22/DBU): Clusters automatizados, 50% más económico

3. **Fórmula de DBUs:**
   ```
   DBUs por hora = Cores × 0.75 × Num Workers
   ```
   - Factor 0.75: Ajuste de Databricks basado en capacidad efectiva

**Ejemplo de cálculo:**
```
Cluster: 3 workers × Standard_DS3_v2 (4 cores, 14GB RAM)
Tipo: Jobs (automated)
Spot: Sí
Horas: 160/mes

Costos por hora:
- VM: 3 × $0.192 × 0.60 (spot) = $0.346
- DBU: (4 × 0.75 × 3) × $0.22 = $1.98
- Total: $2.33/hora

Costo mensual: $2.33 × 160 = $372.80
Ahorro vs No-Spot: 40%
Ahorro vs All-Purpose: 60%
```

**Estrategias de optimización:**
- ✅ Usar Jobs clusters en lugar de All-Purpose cuando sea posible
- ✅ Habilitar Spot VMs para workloads tolerantes a interrupciones
- ✅ Configurar auto-termination (ej: 30 minutos de inactividad)
- ✅ Usar autoscaling para ajustar workers dinámicamente
- ✅ Pool de clusters para reducir tiempo de inicio

---

## 📓 Lab 03 - Notebooks Avanzados

### Magic Commands

**¿Qué son los Magic Commands?**
- Comandos especiales precedidos por `%` (línea) o `%%` (celda)
- Ejecutan operaciones fuera del lenguaje principal del notebook
- Heredados de Jupyter, extendidos por Databricks

**Magic Commands principales:**

1. **`%md` - Markdown**
```python
%md
# Título
Este texto se renderiza como **Markdown**
- Lista item 1
- Lista item 2
```
- Permite formatear texto con Markdown
- Útil para documentación inline
- Soporta: títulos, listas, negritas, código inline, links

2. **`%sql` - SQL Query**
```python
%sql
SELECT * FROM tabla WHERE condicion = 'valor'
```
- Ejecuta SQL sobre temp views o tablas Delta
- Resultados se visualizan automáticamente
- Puede asignar resultado a variable:
  ```python
  resultado = spark.sql("SELECT * FROM tabla")
  ```

3. **`%sh` - Shell Commands**
```python
%sh
ls -la
pwd
echo "Variable: $MY_VAR"
```
- Ejecuta comandos de shell Unix
- Útil para: ver archivos, ejecutar scripts, diagnosticar
- ⚠️ Se ejecuta en driver, no en workers

4. **`%fs` - File System**
```python
%fs ls /tmp/
%fs head /path/to/file.csv
%fs rm -r /path/to/directory
```
- Comandos sobre Databricks File System (DBFS)
- Similar a comandos Unix pero para DBFS
- Alternativa: `dbutils.fs.*` en Python

5. **`%run` - Run Notebook**
```python
%run /path/to/other/notebook
```
- Ejecuta otro notebook en el contexto actual
- Variables del notebook ejecutado quedan disponibles
- Útil para: librerías compartidas, configuraciones

6. **`%pip` - Install Packages**
```python
%pip install pandas==1.5.0
%pip install -r requirements.txt
```
- Instala paquetes de Python en el notebook
- Instalación solo para la sesión actual
- Para instalación permanente: usar librerías del cluster

---

### Widgets (Parametrización)

**¿Qué son los Widgets?**
- Controles UI para parametrizar notebooks
- Permiten input de usuarios sin modificar código
- Útiles para: desarrollo iterativo, jobs parametrizados

**Tipos de Widgets:**

1. **Text Widget**
```python
dbutils.widgets.text("param_name", "default_value", "Label")
value = dbutils.widgets.get("param_name")
```
- Input de texto libre
- Uso: fechas, strings, números como texto

2. **Dropdown Widget**
```python
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
env = dbutils.widgets.get("env")
```
- Selección de una opción de lista cerrada
- Uso: ambientes, regiones, opciones fijas

3. **Combobox Widget**
```python
dbutils.widgets.combobox("region", "US", ["US", "EU", "APAC"], "Region")
```
- Dropdown + permite input custom
- Uso: listas con opción de agregar valores

4. **Multiselect Widget**
```python
dbutils.widgets.multiselect("columns", "col1,col2", ["col1", "col2", "col3"], "Columns")
selected = dbutils.widgets.get("columns").split(",")
```
- Selección de múltiples opciones
- Retorna string separado por comas
- Requiere `.split(",")` para obtener lista

**Remover Widgets:**
```python
dbutils.widgets.remove("param_name")  # Remover uno
dbutils.widgets.removeAll()           # Remover todos
```

**Uso en Jobs:**
```python
# En Databricks Jobs UI, puedes pasar parámetros:
# {"process_date": "2026-05-22", "env": "prod"}

# El notebook los recibe automáticamente
process_date = dbutils.widgets.get("process_date")
env = dbutils.widgets.get("env")
```

---

### Debugging y Logging

**1. display() vs print()**
```python
# print(): Output simple de texto
print(f"Procesando {count} registros")

# display(): Visualización rica para DataFrames/gráficos
display(df)  # Tabla interactiva + opciones de visualización
```

**2. Profiling y Timing**
```python
import time
from functools import wraps

def time_it(func):
    """Decorator para medir tiempo de ejecución"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"⏱️  {func.__name__} ejecutado en {end - start:.2f}s")
        return result
    return wrapper

@time_it
def procesar_datos(df):
    return df.groupBy("category").count()

# Uso:
result = procesar_datos(df_large)
```

**3. explain() - Query Plans**
```python
# Ver plan de ejecución ANTES de ejecutar
df.filter(col("amount") > 1000).groupBy("category").count().explain()

# Ver plan extendido (con estadísticas)
df.explain(mode="extended")

# Ver plan formatado
df.explain(mode="formatted")
```

**Tipos de planes:**
- **Parsed**: Query original
- **Analyzed**: Después de resolución de nombres
- **Optimized**: Después de optimizaciones (Catalyst)
- **Physical**: Plan de ejecución real

**4. Spark UI**
- Acceder: Cluster → Spark UI tab
- Información:
  - Jobs: Operaciones de alto nivel
  - Stages: Pasos de cada job
  - Tasks: Unidades de trabajo en executors
  - Storage: DataFrames cacheados
  - SQL: Queries y planes de ejecución

**5. Logging Personalizado**
```python
import logging

# Configurar logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Usar en código
logger.info("Iniciando proceso...")
logger.warning("Datos faltantes detectados")
logger.error("Error en transformación")
```

---

## 📓 Lab 04 - Transformación de Datos (Medallion Architecture)

### Arquitectura Medallion

**¿Qué es Medallion Architecture?**
- Patrón de diseño para Data Lakehouses
- Organiza datos en capas incrementales de calidad
- Cada capa tiene propósito específico

**Capas:**

1. **Bronze (Raw):**
   - Datos crudos "as-is" de fuentes
   - Sin transformaciones (excepto metadata de ingesta)
   - Propósito: Preservar datos originales, auditoría
   - Formato: Delta (ACID) o Parquet
   - Ejemplo: Logs, eventos, archivos CSV importados

2. **Silver (Cleaned):**
   - Datos limpiados y estandarizados
   - Deduplicación, validación, normalización
   - Propósito: Datos confiables para análisis
   - Agregado de columnas técnicas
   - Ejemplo: Transacciones validadas, usuarios activos

3. **Gold (Curated):**
   - Datos agregados para casos de uso específicos
   - Optimizados para consultas de negocio
   - Propósito: Serving layer para BI/ML
   - Puede tener múltiples tablas Gold
   - Ejemplo: Métricas diarias, dashboards, features para ML

**Flujo de datos:**
```
Source → Bronze → Silver → Gold → BI/ML
         (Raw)   (Clean)  (Agg)
```

---

### Generación de Datos Sintéticos

```python
def generate_sales_data(num_records=1000, batch_id="BATCH_001"):
    """
    Genera datos sintéticos de ventas para demostración.
    
    Args:
        num_records (int): Número de registros a generar
        batch_id (str): Identificador del lote de datos
    
    Returns:
        DataFrame: Datos de ventas con metadata de ingesta
    
    Columnas generadas:
        - transaction_id: ID único de transacción
        - date: Fecha de transacción (últimos 30 días)
        - product_code: Código de producto (P100-P999)
        - product_name: Nombre de producto
        - region: Región de venta (NORTH/SOUTH/EAST/WEST)
        - amount: Monto de venta (50-1000)
        - quantity: Cantidad vendida (1-10)
        - customer_id: ID de cliente
        - status: Estado (completed/pending/cancelled)
        - _ingested_at: Timestamp de ingesta
        - _source: Sistema fuente
        - _batch_id: ID del lote
    """
    dates = [(datetime.now() - timedelta(days=x)) for x in range(30)]
    products = ["LAPTOP", "MOUSE", "KEYBOARD", "MONITOR", "WEBCAM"]
    regions = ["NORTH", "SOUTH", "EAST", "WEST"]
    
    data = []
    for i in range(num_records):
        data.append({
            "transaction_id": f"TXN{i:06d}",  # TXN000000, TXN000001, ...
            "date": dates[random.randint(0, len(dates)-1)].strftime("%Y-%m-%d"),
            "product_code": f"P{random.randint(100, 999)}",
            "product_name": random.choice(products),
            "region": random.choice(regions),
            "amount": round(random.uniform(50, 1000), 2),
            "quantity": random.randint(1, 10),
            "customer_id": f"CUST{random.randint(1000, 9999)}",
            "status": random.choice(["completed", "pending", "cancelled"])
        })
    
    df = spark.createDataFrame(data)
    
    # Agregar metadata de ingesta (patrón Bronze)
    df_bronze = df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("sales_system")) \
        .withColumn("_batch_id", lit(batch_id))
    
    return df_bronze
```

**Metadata de ingesta:**
- `_ingested_at`: Timestamp de carga (para tracking)
- `_source`: Sistema de origen (para lineage)
- `_batch_id`: Identificador de lote (para reprocessing)

**Convenciones de naming:**
- Prefijo `_`: Columnas técnicas/metadata
- Distingue datos de negocio de datos operacionales

---

### Deduplicación con Window Functions

```python
from pyspark.sql.window import Window

# Definir ventana de particionamiento
window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingested_at").desc())

# Aplicar row_number() sobre la ventana
df_dedup = df_bronze \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")
```

**Explicación detallada:**

1. **Window Functions:**
   - Operan sobre un "marco" de filas relacionadas
   - Similar a SQL window functions
   - Permiten cálculos por grupo sin reducir filas

2. **`Window.partitionBy("transaction_id")`:**
   - Agrupa filas por transaction_id
   - Cada grupo se procesa independientemente

3. **`.orderBy(col("_ingested_at").desc())`:**
   - Dentro de cada partición, ordena por timestamp
   - `.desc()`: Orden descendente (más reciente primero)

4. **`row_number().over(window_spec)`:**
   - Asigna número secuencial (1, 2, 3...) a cada fila en la ventana
   - La primera fila (más reciente) recibe número 1

5. **`.filter(col("row_num") == 1)`:**
   - Mantiene solo la fila más reciente de cada transaction_id
   - Elimina duplicados

**Alternativas de deduplicación:**

```python
# Opción 1: rank() - Permite empates
window = Window.partitionBy("id").orderBy(col("timestamp").desc())
df.withColumn("rank", rank().over(window)).filter(col("rank") == 1)

# Opción 2: dropDuplicates() - Más simple pero menos control
df.dropDuplicates(["transaction_id"])

# Opción 3: Mantener el último por clave completa
df.groupBy("transaction_id").agg(max(struct(*[col(c) for c in df.columns])).alias("latest"))
```

---

### Data Quality Checks

```python
def validate_data_quality(df):
    """
    Valida calidad de datos y marca registros inválidos.
    
    Validaciones aplicadas:
        - transaction_id no debe ser nulo
        - amount debe ser mayor a 0
        - quantity debe ser mayor a 0
        - status debe estar en lista válida
    
    Returns:
        DataFrame con columnas:
            - quality_issues (array): Lista de problemas encontrados
            - is_valid (boolean): True si pasa todas las validaciones
    """
    df_validated = df \
        .withColumn("quality_issues", array()) \
        .withColumn("quality_issues",
                    when(col("transaction_id").isNull(), 
                         array_union(col("quality_issues"), array(lit("missing_transaction_id"))))
                    .otherwise(col("quality_issues"))) \
        .withColumn("quality_issues",
                    when(col("amount") <= 0, 
                         array_union(col("quality_issues"), array(lit("invalid_amount"))))
                    .otherwise(col("quality_issues"))) \
        .withColumn("quality_issues",
                    when(col("quantity") <= 0, 
                         array_union(col("quality_issues"), array(lit("invalid_quantity"))))
                    .otherwise(col("quality_issues"))) \
        .withColumn("quality_issues",
                    when(~col("status").isin(["completed", "pending", "cancelled"]), 
                         array_union(col("quality_issues"), array(lit("invalid_status"))))
                    .otherwise(col("quality_issues"))) \
        .withColumn("is_valid", size(col("quality_issues")) == 0)
    
    return df_validated
```

**Explicación paso a paso:**

1. **Inicializar columna de issues:**
```python
.withColumn("quality_issues", array())
```
- Crea array vacío para acumular problemas
- Tipo de dato: ArrayType(StringType())

2. **Agregar issues condicionalmente:**
```python
.withColumn("quality_issues",
    when(condicion_falla, 
         array_union(col("quality_issues"), array(lit("nombre_issue"))))
    .otherwise(col("quality_issues")))
```
- `when(condicion, valor_si_true).otherwise(valor_si_false)`
- `array_union()`: Combina dos arrays (sin duplicados)
- `array(lit("texto"))`: Crea array con un elemento

3. **Marcar validez:**
```python
.withColumn("is_valid", size(col("quality_issues")) == 0)
```
- `size()`: Retorna longitud del array
- Si longitud == 0, no hay issues → válido

**Separar registros válidos e inválidos:**
```python
df_silver = df_validated.filter(col("is_valid")).drop("quality_issues", "is_valid")
df_errors = df_validated.filter(~col("is_valid"))  # ~ = NOT
```

**Patrón de error handling:**
- Registros válidos → Silver (continúan pipeline)
- Registros inválidos → Tabla de errores (para investigación)
- Permite procesamiento tolerante a fallos

---

### Delta Lake Operations

#### MERGE (Upsert)

```python
from delta.tables import DeltaTable

# Cargar tabla Delta existente
delta_table = DeltaTable.forPath(spark, silver_path)

# Ejecutar MERGE
delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.transaction_id = source.transaction_id"  # Condición de join
).whenMatchedUpdateAll() \  # Si existe, actualizar todas las columnas
 .whenNotMatchedInsertAll() \  # Si no existe, insertar
 .execute()
```

**Explicación:**
- **MERGE**: Operación UPSERT (UPDATE + INSERT)
- **whenMatchedUpdateAll()**: Actualiza todas las columnas si hay match
- **whenNotMatchedInsertAll()**: Inserta si no hay match
- **Alternativas:**
  - `.whenMatchedUpdate(set={...})`: Actualizar columnas específicas
  - `.whenMatchedDelete()`: Eliminar si cumple condición
  - `.whenNotMatchedInsert(values={...})`: Insertar valores específicos

**Ejemplo con condiciones:**
```python
delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition="source.timestamp > target.timestamp",  # Solo si es más reciente
    set={"value": "source.value", "updated_at": "current_timestamp()"}
).whenNotMatchedInsert(
    values={"id": "source.id", "value": "source.value", "created_at": "current_timestamp()"}
).execute()
```

#### OPTIMIZE (Compactación)

```python
# Compactar archivos pequeños
spark.sql(f"OPTIMIZE delta.`{silver_path}`")
```

**¿Por qué OPTIMIZE?**
- Escrituras frecuentes generan muchos archivos pequeños
- Archivos pequeños → Performance degradado en lecturas
- OPTIMIZE combina archivos en archivos más grandes
- Target: 1GB por archivo (configurable)

**Cuándo ejecutar:**
- Después de muchas escrituras pequeñas
- Antes de lecturas intensivas
- En schedule (ej: diario durante off-hours)

#### Z-ORDER (Indexación)

```python
# Z-ORDER por columnas frecuentemente filtradas
spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (region, date, product_code)")
```

**¿Qué es Z-ORDER?**
- Técnica de indexación multidimensional
- Coloca datos relacionados cerca físicamente
- Mejora performance de filtros y joins

**Cuándo usar:**
- Columnas frecuentemente usadas en WHERE
- Columnas con alta cardinalidad (muchos valores únicos)
- Máximo 4-5 columnas (orden importa: más selectiva primero)

**Ejemplo de impacto:**
```python
# Sin Z-ORDER: Escanea todos los archivos
df.filter((col("region") == "NORTH") & (col("date") == "2026-05-22")).count()

# Con Z-ORDER: Escanea solo archivos relevantes (data skipping)
# Puede ser 10-100x más rápido dependiendo de datos
```

#### DESCRIBE HISTORY

```python
# Ver historial de cambios
history = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`")
display(history.select("version", "timestamp", "operation", "operationParameters"))
```

**Información disponible:**
- **version**: Número de versión (incremental)
- **timestamp**: Cuándo se hizo el cambio
- **operation**: Tipo (WRITE, MERGE, DELETE, UPDATE, OPTIMIZE)
- **operationParameters**: Detalles de la operación
- **userMetadata**: Metadata custom
- **readVersion/isolationLevel**: Detalles técnicos

**Uso:**
- Auditoría de cambios
- Debugging de problemas
- Cumplimiento regulatorio

#### TIME TRAVEL

```python
# Leer versión específica
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)

# Leer en timestamp específico
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2026-05-21").load(silver_path)

# Comparar versiones
df_v0.count() - df_latest.count()  # Cuántos registros se agregaron
```

**Casos de uso:**
- Rollback a versión anterior
- Análisis de cambios históricos
- Reproducir resultados pasados
- Auditoría y compliance

**VACUUM (Cleanup):**
```python
# Eliminar archivos viejos (por defecto: 7 días de retención)
spark.sql(f"VACUUM delta.`{silver_path}`")

# Cambiar retención (ej: 30 días)
spark.sql(f"VACUUM delta.`{silver_path}` RETAIN 720 HOURS")
```
⚠️ VACUUM elimina archivos, impide time travel a versiones eliminadas

---

## 📓 Lab 05 - Jobs y Workflows

### Parametrización con Widgets

**Notebook parametrizado (Task):**
```python
# Definir widgets al inicio del notebook
dbutils.widgets.text("process_date", "2026-05-22", "📅 Fecha de Proceso")
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "🌍 Environment")
dbutils.widgets.text("batch_size", "1000", "📦 Batch Size")

# Leer valores
process_date = dbutils.widgets.get("process_date")
env = dbutils.widgets.get("env")
batch_size = int(dbutils.widgets.get("batch_size"))
```

**¿Por qué parametrizar?**
- Reutilización de notebooks
- Testing con diferentes inputs
- Ejecución programada con parámetros
- Separación de configuración y lógica

---

### Retornar Resultados Estructurados

```python
import json
from datetime import datetime

try:
    # Lógica de procesamiento...
    output_path = f"/tmp/jobs/{env}/bronze/{process_date}"
    df.write.format("delta").mode("overwrite").save(output_path)
    
    # Preparar resultado exitoso
    result = {
        "status": "SUCCESS",
        "task": "ingest",
        "records_ingested": df.count(),
        "output_path": output_path,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Retornar como JSON
    dbutils.notebook.exit(json.dumps(result))
    
except Exception as e:
    # Manejar errores
    error_result = {
        "status": "FAILED",
        "task": "ingest",
        "error": str(e),
        "timestamp": datetime.utcnow().isoformat()
    }
    dbutils.notebook.exit(json.dumps(error_result))
```

**Explicación:**

1. **`dbutils.notebook.exit()`:**
   - Termina ejecución del notebook
   - Retorna valor al notebook llamador
   - Similar a `return` en funciones

2. **`json.dumps()`:**
   - Serializa dict a string JSON
   - Permite pasar estructuras complejas
   - Notebook receptor usa `json.loads()` para parsear

3. **Estructura del resultado:**
   - `status`: "SUCCESS" o "FAILED"
   - `task`: Nombre de la tarea
   - Métricas específicas (ej: `records_ingested`)
   - `output_path`: Dónde se guardaron los datos
   - `timestamp`: Cuándo se ejecutó

**Llamar notebook desde otro notebook:**
```python
# Ejecutar notebook hijo
result_json = dbutils.notebook.run(
    "/path/to/notebook",
    timeout_seconds=3600,
    arguments={"process_date": "2026-05-22", "env": "prod"}
)

# Parsear resultado
result = json.loads(result_json)
if result["status"] == "SUCCESS":
    print(f"✅ Task completada: {result['records_ingested']} registros")
else:
    print(f"❌ Task falló: {result['error']}")
```

---

### Orquestación Multi-Task

**Patrón de orquestador:**
```python
import json
from datetime import datetime

# Parámetros compartidos
params = {
    "process_date": "2026-05-22",
    "env": "dev",
    "batch_size": "1000"
}

workflow_results = {}

# Task 1: Ingest
print("🔹 Ejecutando Task 1: Ingest")
result1_json = dbutils.notebook.run("/path/to/Task1_Ingest", 0, params)
result1 = json.loads(result1_json)
workflow_results["task1"] = result1

if result1["status"] != "SUCCESS":
    raise Exception(f"Task 1 falló: {result1['error']}")

# Task 2: Transform (depende de Task 1)
print("🔹 Ejecutando Task 2: Transform")
result2_json = dbutils.notebook.run("/path/to/Task2_Transform", 0, params)
result2 = json.loads(result2_json)
workflow_results["task2"] = result2

if result2["status"] != "SUCCESS":
    raise Exception(f"Task 2 falló: {result2['error']}")

# Task 3: Aggregate (depende de Task 2)
print("🔹 Ejecutando Task 3: Aggregate")
result3_json = dbutils.notebook.run("/path/to/Task3_Aggregate", 0, params)
result3 = json.loads(result3_json)
workflow_results["task3"] = result3

# Resumen final
print("\\n📊 RESUMEN DEL WORKFLOW")
for task_name, result in workflow_results.items():
    status_icon = "✅" if result["status"] == "SUCCESS" else "❌"
    print(f"{status_icon} {task_name}: {result['status']}")
```

**En Databricks Jobs UI:**
- Crear Job con múltiples Tasks
- Configurar dependencias (Task 2 depends on Task 1)
- Pasar parámetros a nivel de Job
- Configurar alertas por email/Slack
- Schedule (cron expression)

**Ventajas de Jobs UI vs notebooks:**
- Manejo de dependencias visual
- Retry automático en failures
- Alertas integradas
- Logs centralizados
- Monitoring de costo por job

---

## 📓 Lab 06 - Integración y Streaming

### Lectura de Múltiples Formatos

#### CSV
```python
df_csv = spark.read.format("csv") \
    .option("header", "true") \      # Primera fila como nombres de columnas
    .option("inferSchema", "true") \  # Inferir tipos de datos
    .option("delimiter", ",") \       # Delimitador (default: coma)
    .option("quote", "\"") \          # Carácter de quote
    .option("escape", "\\\\") \        # Carácter de escape
    .option("nullValue", "NULL") \    # Representación de nulos
    .load(path)
```

**Opciones comunes:**
- `header`: True si primera fila tiene nombres
- `inferSchema`: True para inferir tipos (más lento pero conveniente)
- `schema`: Especificar schema manualmente (más rápido)
- `sep` o `delimiter`: Carácter separador
- `mode`: "PERMISSIVE" (default), "DROPMALFORMED", "FAILFAST"

#### JSON
```python
df_json = spark.read.format("json") \
    .option("multiLine", "false") \   # False: cada línea es un JSON
    .option("primitivesAsString", "false") \
    .option("prefersDecimal", "true") \
    .load(path)
```

**Tipos de JSON:**
- **Line-delimited** (NDJSON): Cada línea es un JSON object
  ```
  {"id": 1, "name": "Alice"}
  {"id": 2, "name": "Bob"}
  ```
- **Multi-line**: Un solo JSON object en múltiples líneas
  ```json
  {
    "id": 1,
    "name": "Alice"
  }
  ```

#### Parquet
```python
df_parquet = spark.read.format("parquet") \
    .option("mergeSchema", "false") \  # Combinar schemas de múltiples archivos
    .load(path)
```

**Ventajas de Parquet:**
- Formato columnar (lee solo columnas necesarias)
- Compresión eficiente (snappy, gzip, lzo)
- Preserva schema y tipos
- Compatible con múltiples herramientas

---

### Joins Complejos

```python
# Join 1: Transacciones + Productos
df_enriched = df_transactions \
    .join(df_products, "product_id", "left")

# Join 2: Agregar Clientes
df_complete = df_enriched \
    .join(df_customers, "customer_id", "left")
```

**Tipos de Join:**
| Tipo | SQL | Descripción |
|------|-----|-------------|
| inner | INNER JOIN | Solo registros con match en ambos |
| left (left_outer) | LEFT JOIN | Todos de izquierda + matches |
| right (right_outer) | RIGHT JOIN | Todos de derecha + matches |
| outer (full_outer) | FULL OUTER JOIN | Todos de ambos lados |
| left_semi | LEFT SEMI JOIN | Izquierda donde hay match (sin columnas de derecha) |
| left_anti | LEFT ANTI JOIN | Izquierda donde NO hay match |
| cross | CROSS JOIN | Producto cartesiano |

**Performance de Joins:**
1. **Broadcast Join** (más rápido):
   - Tabla pequeña (<10MB) se copia a todos los executors
   - Evita shuffle
   - Spark lo hace automáticamente si tabla es pequeña
   - Forzar: `df_large.join(broadcast(df_small), "key")`

2. **Shuffle Join** (más lento):
   - Ambas tablas son grandes
   - Requiere reparticionamiento por clave
   - Costoso en I/O de red

**Optimizaciones:**
```python
# Pre-filtrar antes de join (reduce datos a mover)
df_transactions.filter(col("date") >= "2026-01-01").join(...)

# Repartition por clave de join
df1.repartition("join_key").join(df2.repartition("join_key"), "join_key")

# Usar broadcast para tablas pequeñas
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "key")
```

---

### Particionado Estratégico

```python
# Guardar con particionado
df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("category", "country") \
    .save(path)
```

**¿Por qué particionar?**
- **Pruning**: Solo lee particiones relevantes
- **Paralelismo**: Escritura/lectura en paralelo por partición
- **Organización**: Datos agrupados lógicamente

**Directorio resultante:**
```
/path/
  category=Electronics/
    country=US/
      part-00000.parquet
    country=UK/
      part-00000.parquet
  category=Clothing/
    country=US/
      part-00000.parquet
```

**Best Practices:**
- ✅ Particionar por columnas frecuentemente filtradas
- ✅ Cardinalidad media (10-10,000 valores únicos)
- ✅ Evitar demasiadas particiones (small files problem)
- ❌ No particionar por columnas de alta cardinalidad (ej: transaction_id)
- ❌ No más de 2-3 niveles de particionado

**Ejemplo de impacto:**
```python
# Sin particionado: Escanea TODO
df = spark.read.format("delta").load(path)
df.filter(col("country") == "US").count()  # Lee todos los archivos

# Con particionado: Solo lee particiones de US
df.filter(col("country") == "US").count()  # Lee solo archivos en country=US/
```

---

### Streaming con Auto Loader

```python
# Configurar stream
df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .load(input_path)

# Transformar
df_processed = df_stream.withColumn("processed_at", current_timestamp())

# Escribir stream
query = df_processed.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start(output_path)
```

**Auto Loader (cloudFiles):**
- Monitorea directorio y procesa archivos nuevos automáticamente
- Schema evolution: Detecta cambios en schema
- Exactly-once semantics con checkpointing
- Más eficiente que directory listing para miles de archivos

**Opciones importantes:**
- `cloudFiles.format`: Formato de archivos (json, csv, parquet, etc.)
- `cloudFiles.schemaLocation`: Donde guardar schema inferido
- `cloudFiles.useNotifications`: Usar eventos de storage (más eficiente)

**Output Modes:**
- **append**: Solo nuevas filas (default para streaming)
- **complete**: Toda la tabla agregada (requiere agregaciones)
- **update**: Solo filas actualizadas (para tablas con aggregaciones)

**Checkpointing:**
- Guarda estado del stream (offsets procesados)
- Permite recovery en caso de falla
- Garantiza exactly-once processing
- ⚠️ No cambiar checkpoint location sin razón

**Monitoring:**
```python
# Ver status
query.status

# Ver métricas recientes
query.recentProgress

# Detener stream
query.stop()

# Esperar a que termine
query.awaitTermination()
```

---

## 🎯 Mejores Prácticas Generales

### 1. Performance

**Cache estratégico:**
```python
# Cachear DataFrames usados múltiples veces
df_large.cache()
df_large.count()  # Materializar cache

result1 = df_large.filter(...).count()
result2 = df_large.groupBy(...).count()  # Usa cache

df_large.unpersist()  # Liberar memoria
```

**Evitar shuffles innecesarios:**
```python
# ❌ MALO: Múltiples shuffles
df.repartition(10).filter(...).groupBy(...).count()

# ✅ BUENO: Filtrar primero
df.filter(...).groupBy(...).count()
```

**Usar funciones nativas de Spark:**
```python
# ❌ MALO: UDF (lento)
from pyspark.sql.functions import udf
@udf("string")
def upper_udf(s):
    return s.upper()
df.withColumn("upper", upper_udf(col("name")))

# ✅ BUENO: Función nativa
df.withColumn("upper", upper(col("name")))
```

### 2. Manejo de Errores

**Try-except en notebooks:**
```python
try:
    # Lógica principal
    df.write.save(path)
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS"}))
except Exception as e:
    # Logging
    logger.error(f"Error: {str(e)}")
    # Cleanup si es necesario
    dbutils.fs.rm(temp_path, True)
    # Retornar error
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)}))
```

### 3. Seguridad

**Usar secretos:**
```python
# ❌ NUNCA hardcodear credenciales
password = "mypassword123"

# ✅ Usar Azure Key Vault via secret scope
password = dbutils.secrets.get(scope="my-scope", key="password")
```

### 4. Logging y Monitoreo

**Logging estructurado:**
```python
import logging
import json

logger = logging.getLogger(__name__)

# Log con contexto
logger.info(json.dumps({
    "event": "processing_started",
    "process_date": process_date,
    "env": env,
    "timestamp": datetime.utcnow().isoformat()
}))
```

### 5. Testing

**Unit tests para funciones:**
```python
def test_validate_data_quality():
    # Crear datos de prueba
    data = [("TXN001", 100, 1, "completed")]
    df = spark.createDataFrame(data, ["transaction_id", "amount", "quantity", "status"])
    
    # Ejecutar función
    df_result = validate_data_quality(df)
    
    # Assertions
    assert df_result.filter(col("is_valid")).count() == 1
    assert df_result.filter(col("is_valid") == False).count() == 0
```

---

## 📚 Referencias y Recursos Adicionales

**Documentación Oficial:**
- [PySpark API Docs](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Guide](https://docs.delta.io/)

**Funciones más usadas:**
- **Transformaciones**: select, filter, withColumn, drop, join, groupBy, orderBy
- **Agregaciones**: count, sum, avg, min, max, collect_list, countDistinct
- **Window**: row_number, rank, dense_rank, lag, lead
- **Strings**: upper, lower, trim, concat, split, regexp_replace
- **Fechas**: current_date, current_timestamp, to_date, date_add, datediff
- **Condicionales**: when, otherwise, coalesce, isnull, isnotnull

**Patterns comunes:**
- Medallion Architecture (Bronze → Silver → Gold)
- Slowly Changing Dimensions (SCD Type 2)
- Idempotent pipelines
- Late-arriving data handling
- Schema evolution

---

**Fin de la documentación**
