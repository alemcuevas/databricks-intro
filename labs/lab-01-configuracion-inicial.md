# Lab 01 - Configuración Inicial y Workspace

## 🎯 Objetivos del Laboratorio

Al finalizar este laboratorio, serás capaz de:
- Acceder a un workspace de Azure Databricks
- Navegar por la interfaz de usuario
- Crear y organizar carpetas
- Importar notebooks
- Configurar permisos básicos

## ⏱️ Tiempo Estimado

30 minutos

## 📋 Prerequisitos

- Acceso a Azure Portal
- Permisos de Contributor en el subscription/resource group
- Navegador web actualizado (Chrome, Edge, Firefox)

---

## Parte 1: Acceso al Workspace (10 minutos)

### Paso 1.1: Acceder desde Azure Portal

1. Inicia sesión en [Azure Portal](https://portal.azure.com)

2. Navega a tu recurso de Azure Databricks:
   - Busca "Databricks" en la barra de búsqueda
   - Selecciona tu workspace (ej: `databricks-kt-workspace`)

3. Haz clic en "Launch Workspace"

   ```
   ┌─────────────────────────────────────┐
   │  databricks-kt-workspace            │
   ├─────────────────────────────────────┤
   │                                     │
   │  Resource group: rg-databricks-kt   │
   │  Location: East US                  │
   │  Pricing Tier: Premium              │
   │                                     │
   │  [Launch Workspace]  [Delete]       │
   │                                     │
   └─────────────────────────────────────┘
   ```

### Paso 1.2: Familiarización con la Interface

Una vez dentro del workspace, identifica las secciones principales:

```
┌──────────────────────────────────────────────────────┐
│  Databricks  [tu-workspace]         👤 Usuario       │
├──────────────────────────────────────────────────────┤
│                                                      │
│  ┌────────────┐  ┌─────────────────────────────┐    │
│  │ SIDEBAR    │  │   ÁREA PRINCIPAL            │    │
│  │            │  │                             │    │
│  │ Workspace  │  │   Aquí aparece el contenido │    │
│  │ Repos      │  │   según la sección          │    │
│  │ Data       │  │   seleccionada              │    │
│  │ Compute    │  │                             │    │
│  │ Workflows  │  │                             │    │
│  │ SQL        │  │                             │    │
│  │                                              │    │
│  └────────────┘  └─────────────────────────────┘    │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**Explora cada sección**:

| Sección | Descripción | Qué Hacer |
|---------|-------------|-----------|
| 🏠 **Workspace** | Notebooks, carpetas | Clic para ver estructura |
| 📁 **Repos** | Integración Git | Explorar (puede estar vacío) |
| 📊 **Data** | Bases de datos, tablas | Ver "default" database |
| 💻 **Compute** | Clusters | Ver clusters existentes |
| ⚙️ **Workflows** | Jobs | Ver jobs programados |
| 📈 **SQL** | SQL Warehouses | Explorar SQL endpoints |

---

## Parte 2: Organización del Workspace (10 minutos)

### Paso 2.1: Crear Estructura de Carpetas

1. Haz clic en **Workspace** en el sidebar izquierdo

2. Navega a tu carpeta personal:
   ```
   Workspace → Users → tu-email@empresa.com
   ```

3. Crea la siguiente estructura de carpetas:

   **Clic derecho en tu carpeta → Create → Folder**

   Crear estas carpetas:
   - `01_Exploracion`
   - `02_Ingestion`
   - `03_Transformation`
   - `04_Analytics`
   - `Utils`

   Resultado esperado:
   ```
   Users/
   └── tu-email@empresa.com/
       ├── 01_Exploracion/
       ├── 02_Ingestion/
       ├── 03_Transformation/
       ├── 04_Analytics/
       └── Utils/
   ```

### Paso 2.2: Crear tu Primer Notebook

1. En la carpeta `01_Exploracion`, crea un notebook:
   - Clic derecho → Create → Notebook
   - Nombre: `Mi_Primer_Notebook`
   - Default Language: `Python`
   - Cluster: Selecciona uno existente o "Create New Cluster"

2. En la primera celda, escribe:

   ```python
   # MAGIC %md
   # MAGIC # Mi Primer Notebook en Databricks
   # MAGIC 
   # MAGIC Este es un notebook de prueba para el Knowledge Transfer.
   # MAGIC 
   # MAGIC **Autor**: [Tu Nombre]  
   # MAGIC **Fecha**: 2026-05-22
   ```

3. Agrega una celda de código:

   ```python
   # Verificar versión de Spark
   print(f"Versión de Spark: {spark.version}")
   
   # Información del cluster
   print(f"Cluster actual: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")
   
   # Crear un DataFrame simple
   data = [
       ("Alice", 28, "Engineering"),
       ("Bob", 34, "Sales"),
       ("Charlie", 29, "Engineering"),
       ("Diana", 31, "Marketing")
   ]
   
   columns = ["nombre", "edad", "departamento"]
   df = spark.createDataFrame(data, columns)
   
   display(df)
   ```

4. Ejecuta la celda:
   - Presiona `Shift + Enter`
   - O haz clic en el botón de "Run Cell" (▶️)

### Paso 2.3: Explorar el Resultado

Observa:
- ✅ La salida muestra la versión de Spark
- ✅ Aparece una tabla interactiva con los datos
- ✅ Puedes ordenar las columnas haciendo clic en los encabezados

---

## Parte 3: Importar Notebooks de Ejemplo (5 minutos)

### Paso 3.1: Importar desde Archivo

1. Descarga este notebook de ejemplo (copiar contenido):

   **Archivo: `example_transformation.py`**
   ```python
   # Databricks notebook source
   # MAGIC %md
   # MAGIC # Ejemplo: Transformación de Datos
   # MAGIC 
   # MAGIC Este notebook muestra transformaciones básicas con PySpark.

   # COMMAND ----------

   # MAGIC %md
   # MAGIC ## 1. Crear datos de prueba

   # COMMAND ----------

   from pyspark.sql.functions import col, upper, when

   # Datos de ventas
   data_ventas = [
       (1, "Laptop", 1200.50, "norte", "2026-05-01"),
       (2, "Mouse", 25.99, "sur", "2026-05-02"),
       (3, "Teclado", 75.00, "este", "2026-05-02"),
       (4, "Monitor", 350.00, "norte", "2026-05-03"),
       (5, "Laptop", 1150.00, "oeste", "2026-05-03")
   ]

   df_ventas = spark.createDataFrame(
       data_ventas,
       ["id", "producto", "monto", "region", "fecha"]
   )

   display(df_ventas)

   # COMMAND ----------

   # MAGIC %md
   # MAGIC ## 2. Transformaciones

   # COMMAND ----------

   # Normalizar regiones a mayúsculas
   df_transformed = df_ventas \
       .withColumn("region", upper(col("region"))) \
       .withColumn("categoria",
           when(col("monto") > 1000, "Alto Valor")
           .when(col("monto") > 100, "Medio Valor")
           .otherwise("Bajo Valor")
       )

   display(df_transformed)

   # COMMAND ----------

   # MAGIC %md
   # MAGIC ## 3. Agregaciones

   # COMMAND ----------

   # Ventas por región
   df_por_region = df_transformed \
       .groupBy("region") \
       .agg(
           {"monto": "sum", "id": "count"}
       ) \
       .withColumnRenamed("sum(monto)", "total_ventas") \
       .withColumnRenamed("count(id)", "num_transacciones")

   display(df_por_region)
   ```

2. Guardar el archivo como `example_transformation.py`

3. En Databricks:
   - Ve a la carpeta `01_Exploracion`
   - Clic derecho → Import
   - Selecciona el archivo `example_transformation.py`
   - Clic en "Import"

4. Abre el notebook importado y ejecútalo celda por celda

---

## Parte 4: Permisos y Compartición (5 minutos)

### Paso 4.1: Compartir un Notebook

1. Abre tu notebook `Mi_Primer_Notebook`

2. Haz clic en el botón de permisos (icono de candado o "Share") en la parte superior

3. Agrega permisos:
   - Busca a un compañero de equipo
   - Selecciona nivel de permiso:
     - **Can View**: Solo lectura
     - **Can Run**: Puede ejecutar
     - **Can Edit**: Puede modificar
     - **Can Manage**: Control total

4. Clic en "Add" y luego "Done"

### Paso 4.2: Crear una Carpeta Compartida

1. Ve a `Workspace → Shared`

2. Crea una carpeta:
   - Clic derecho → Create → Folder
   - Nombre: `Team_KT`

3. Mueve una copia de tu notebook a esta carpeta:
   - En tu notebook personal, clic derecho
   - Clone → Seleccionar destino: `/Shared/Team_KT/`

---

## 🎯 Ejercicios Prácticos

### Ejercicio 1: Personalizar tu Espacio

**Tarea**: Crea una estructura de carpetas que refleje tu flujo de trabajo típico.

**Ejemplo de estructura avanzada**:
```
tu-email@empresa.com/
├── Development/
│   ├── Experiments/
│   ├── Prototypes/
│   └── Tests/
├── Production/
│   ├── ETL/
│   ├── Analytics/
│   └── ML/
├── Archive/
└── Shared_Utils/
```

### Ejercicio 2: Notebook de Bienvenida

**Tarea**: Crea un notebook llamado `README` en tu carpeta raíz con:

```python
# MAGIC %md
# MAGIC # Mi Workspace - [Tu Nombre]
# MAGIC 
# MAGIC ## 📁 Estructura de Carpetas
# MAGIC 
# MAGIC - `Development/`: Notebooks en desarrollo
# MAGIC - `Production/`: Código productivo
# MAGIC - `Shared_Utils/`: Funciones compartidas
# MAGIC 
# MAGIC ## 🎯 Proyectos Actuales
# MAGIC 
# MAGIC 1. **ETL de Ventas**: [Link al notebook](...)
# MAGIC 2. **Dashboard de Métricas**: [Link al notebook](...)
# MAGIC 
# MAGIC ## 📞 Contacto
# MAGIC 
# MAGIC - Email: tu-email@empresa.com
# MAGIC - Teams: @tu-usuario
```

### Ejercicio 3: Explorar Datos de Ejemplo

**Tarea**: Databricks incluye datasets de ejemplo. Explóralos:

```python
# Listar datasets de ejemplo
display(dbutils.fs.ls("/databricks-datasets/"))

# Leer un dataset de ejemplo: Iris
df_iris = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/databricks-datasets/Rdatasets/data-001/csv/datasets/iris.csv")

display(df_iris)

# Estadísticas básicas
df_iris.describe().show()

# Conteo por especie
df_iris.groupBy("Species").count().show()
```

---

## ✅ Checklist de Completitud

Al terminar este laboratorio, deberías tener:

- [ ] Acceso exitoso al workspace de Databricks
- [ ] Familiarización con la interfaz de usuario
- [ ] Estructura de carpetas creada
- [ ] Al menos 2 notebooks creados
- [ ] 1 notebook importado y ejecutado
- [ ] Permisos compartidos con al menos 1 persona
- [ ] Comprensión de la navegación básica

---

## 🔍 Troubleshooting

### Problema 1: No puedo acceder al workspace

**Solución**:
- Verifica que tienes permisos de Contributor en el subscription
- Verifica que el workspace está en estado "Running"
- Intenta desde el modo incognito del navegador
- Contacta al administrador de Azure

### Problema 2: No aparece el botón "Create Cluster"

**Solución**:
- Verifica que tienes permisos para crear clusters
- Puede que haya una Cluster Policy restrictiva
- Usa un cluster existente por ahora
- Contacta al administrador del workspace

### Problema 3: Error al ejecutar código

**Solución**:
- Verifica que el cluster está en estado "Running"
- Espera a que el cluster termine de iniciar (3-5 minutos)
- Revisa los logs del cluster en la pestaña "Event Log"

---

## 📚 Recursos Adicionales

- [Documentación: Workspace UI](https://docs.microsoft.com/azure/databricks/workspace/)
- [Documentación: Notebooks](https://docs.microsoft.com/azure/databricks/notebooks/)
- [Keyboard Shortcuts](https://docs.microsoft.com/azure/databricks/notebooks/shortcuts)

---

**Siguiente**: [Lab 02 - Creación y Configuración de Clusters](./lab-02-clusters.md)
