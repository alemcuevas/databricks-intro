# Laboratorio 6: Integración con Servicios Azure

⏱️ **Duración estimada**: 45-50 minutos  
🎯 **Nivel**: Avanzado

## 📋 Objetivos

- Conectar con Azure Storage (ADLS Gen2)
- Integrar con Azure Cosmos DB
- Conectar con Azure SQL Database
- Trabajar con Azure Key Vault
- (Bonus) Streaming con Event Hubs
- Implementar autenticación segura

---

## ⚠️ Pre-requisitos

Para este lab necesitas:
- Azure Storage Account (ADLS Gen2 habilitado)
- Azure Cosmos DB account (NoSQL API)
- Azure SQL Database
- Azure Key Vault
- Service Principal con permisos adecuados

**Nota**: Si no tienes estos recursos, puedes seguir el lab conceptualmente y ver el código.

---

## Ejercicio 1: Azure Storage (ADLS Gen2) (15 min)

### Paso 1: Configurar Credenciales en Key Vault

1. **En Azure Portal → Key Vault**
2. **Secrets → Generate/Import**:
   ```
   Name: storage-account-key
   Value: [tu storage account key]
   ```

3. **Service Principal** (opción preferida):
   ```
   Name: sp-client-id
   Value: [application/client ID]
   
   Name: sp-client-secret
   Value: [client secret]
   
   Name: sp-tenant-id
   Value: [tenant ID]
   ```

### Paso 2: Configurar Secret Scope en Databricks

Método: **Azure Key Vault-backed**

1. **En Databricks**, navega a:
   ```
   https://<databricks-instance>#secrets/createScope
   ```

2. **Configurar**:
   ```
   Scope name: azure-key-vault
   DNS Name: https://<keyvault-name>.vault.azure.net/
   Resource ID: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{kv-name}
   ```

3. **Create**

### Paso 3: Conectar con Storage - Método 1 (Access Key)

Notebook: "Lab06_Storage_AccessKey"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Conexión a ADLS Gen2 - Access Key

# COMMAND ----------

# Configurar credenciales desde Key Vault
storage_account = "mystorageaccount"  # Cambiar por tu storage account
container = "data"

storage_key = dbutils.secrets.get(scope="azure-key-vault", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

print("✅ Credenciales configuradas")

# COMMAND ----------

# Listar contenido
storage_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

try:
    files = dbutils.fs.ls(storage_path)
    print(f"📁 Archivos en {container}:")
    for f in files:
        print(f"  - {f.name}")
except Exception as e:
    print(f"⚠️  Error: {e}")
    print(f"Tip: Verifica que el container '{container}' existe")

# COMMAND ----------

# Escribir datos de prueba
from pyspark.sql.functions import *

df_test = spark.range(0, 1000) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("value", rand() * 100)

output_path = f"{storage_path}lab06_test/data.parquet"

df_test.write.format("parquet").mode("overwrite").save(output_path)

print(f"✅ Datos escritos a: {output_path}")

# COMMAND ----------

# Leer datos
df_read = spark.read.parquet(output_path)
print(f"📊 Registros leídos: {df_read.count()}")
display(df_read.limit(10))
```

### Paso 4: Conectar - Método 2 (Service Principal + OAuth)

Notebook: "Lab06_Storage_OAuth"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Conexión a ADLS Gen2 - Service Principal (OAuth)

# COMMAND ----------

# Configuración con Service Principal (más seguro)
storage_account = "mystorageaccount"
container = "data"

# Obtener credenciales desde Key Vault
client_id = dbutils.secrets.get(scope="azure-key-vault", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="azure-key-vault", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="azure-key-vault", key="sp-tenant-id")

# Configurar OAuth
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

print("✅ OAuth configurado")

# COMMAND ----------

# Probar conexión
storage_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

files = dbutils.fs.ls(storage_path)
print(f"📁 Contenido de {container}:")
for f in files:
    print(f"  {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Storage (alternativa para acceso persistente)

# COMMAND ----------

# Mount point
mount_point = "/mnt/datalake"

# Verificar si ya está montado
mounts = [m.mountPoint for m in dbutils.fs.mounts()]

if mount_point in mounts:
    print(f"⚠️  {mount_point} ya existe. Unmounting...")
    dbutils.fs.unmount(mount_point)

# COMMAND ----------

# Montar con OAuth
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

dbutils.fs.mount(
    source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs
)

print(f"✅ Montado en {mount_point}")

# COMMAND ----------

# Usar mount point
dbutils.fs.ls(mount_point)

# Ahora puedes usar rutas simples
df = spark.read.parquet(f"{mount_point}/lab06_test/data.parquet")
print(f"📊 Leído desde mount: {df.count()} registros")
```

---

## Ejercicio 2: Azure Cosmos DB (15 min)

### Paso 1: Configurar Conexión a Cosmos DB

Notebook: "Lab06_CosmosDB"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Integración con Azure Cosmos DB

# COMMAND ----------

# Parámetros de Cosmos DB
cosmos_endpoint = "https://<your-cosmos-account>.documents.azure.com:443/"
cosmos_database = "SalesDB"
cosmos_container = "transactions"

# Obtener master key desde Key Vault
cosmos_key = dbutils.secrets.get(scope="azure-key-vault", key="cosmos-master-key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Escribir a Cosmos DB

# COMMAND ----------

from pyspark.sql.functions import *

# Crear datos de prueba
df_sales = spark.range(0, 100) \
    .withColumn("transaction_id", concat(lit("TXN"), col("id"))) \
    .withColumn("customer_id", concat(lit("CUST"), (rand() * 1000).cast("int"))) \
    .withColumn("product", 
                when(col("id") % 4 == 0, "Laptop")
                .when(col("id") % 4 == 1, "Mouse")
                .when(col("id") % 4 == 2, "Keyboard")
                .otherwise("Monitor")) \
    .withColumn("amount", (rand() * 1000).cast("decimal(10,2)")) \
    .withColumn("timestamp", current_timestamp()) \
    .drop("id")

print("📊 Datos a escribir:")
display(df_sales.limit(10))

# COMMAND ----------

# Escribir a Cosmos DB
df_sales.write \
    .format("cosmos.oltp") \
    .option("spark.synapse.linkedService", cosmos_endpoint) \
    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .option("spark.cosmos.accountKey", cosmos_key) \
    .option("spark.cosmos.database", cosmos_database) \
    .option("spark.cosmos.container", cosmos_container) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .option("spark.cosmos.write.bulk.enabled", "true") \
    .mode("append") \
    .save()

print("✅ Datos escritos a Cosmos DB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer desde Cosmos DB

# COMMAND ----------

df_cosmos = spark.read \
    .format("cosmos.oltp") \
    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .option("spark.cosmos.accountKey", cosmos_key) \
    .option("spark.cosmos.database", cosmos_database) \
    .option("spark.cosmos.container", cosmos_container) \
    .option("spark.cosmos.read.inferSchema.enabled", "true") \
    .load()

print(f"📊 Registros en Cosmos DB: {df_cosmos.count()}")
display(df_cosmos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query con filtros (pushdown)

# COMMAND ----------

# Filtrar en Cosmos DB (server-side)
df_filtered = spark.read \
    .format("cosmos.oltp") \
    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .option("spark.cosmos.accountKey", cosmos_key) \
    .option("spark.cosmos.database", cosmos_database) \
    .option("spark.cosmos.container", cosmos_container) \
    .option("spark.cosmos.read.inferSchema.enabled", "true") \
    .option("spark.cosmos.read.customQuery", "SELECT * FROM c WHERE c.amount > 500") \
    .load()

print(f"📊 Transacciones > $500: {df_filtered.count()}")
display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregación y Escritura de Resumen

# COMMAND ----------

# Calcular métricas
df_summary = df_cosmos \
    .groupBy("product") \
    .agg(
        count("*").alias("transactions"),
        sum("amount").alias("total_sales"),
        avg("amount").alias("avg_sale")
    ) \
    .withColumn("total_sales", round(col("total_sales"), 2)) \
    .withColumn("avg_sale", round(col("avg_sale"), 2))

print("📊 Resumen por Producto:")
display(df_summary)

# COMMAND ----------

# Guardar resumen en otro container
summary_container = "product_summary"

df_summary.write \
    .format("cosmos.oltp") \
    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
    .option("spark.cosmos.accountKey", cosmos_key) \
    .option("spark.cosmos.database", cosmos_database) \
    .option("spark.cosmos.container", summary_container) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .mode("overwrite") \
    .save()

print(f"✅ Resumen guardado en container: {summary_container}")
```

---

## Ejercicio 3: Azure SQL Database (10 min)

Notebook: "Lab06_AzureSQL"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Integración con Azure SQL Database

# COMMAND ----------

# Parámetros SQL
sql_server = "<server-name>.database.windows.net"
sql_database = "SalesDB"
sql_table = "dbo.Sales"
sql_user = "sqladmin"

# Password desde Key Vault
sql_password = dbutils.secrets.get(scope="azure-key-vault", key="sql-password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer desde Azure SQL

# COMMAND ----------

# JDBC URL
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database}"

# Leer tabla
df_sql = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", sql_table) \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

print(f"📊 Registros en SQL: {df_sql.count()}")
display(df_sql.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer con Query Personalizado

# COMMAND ----------

# Query con filtros y joins
custom_query = """
(SELECT 
    s.SaleID,
    s.ProductID,
    p.ProductName,
    s.Amount,
    s.SaleDate
FROM dbo.Sales s
JOIN dbo.Products p ON s.ProductID = p.ProductID
WHERE s.SaleDate >= '2026-01-01'
) as query
"""

df_custom = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", custom_query) \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .load()

print(f"📊 Registros con JOIN: {df_custom.count()}")
display(df_custom)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura Particionada (Performance)

# COMMAND ----------

# Leer en paralelo usando partitioning por columna numérica
df_partitioned = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", sql_table) \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("partitionColumn", "SaleID") \
    .option("lowerBound", "1") \
    .option("upperBound", "100000") \
    .option("numPartitions", "8") \
    .load()

print(f"📊 Particiones: {df_partitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escribir a Azure SQL

# COMMAND ----------

from pyspark.sql.functions import *

# Crear datos agregados para escribir
df_agg = df_sql \
    .groupBy("ProductID") \
    .agg(
        count("*").alias("TotalSales"),
        sum("Amount").alias("TotalRevenue"),
        avg("Amount").alias("AvgAmount")
    )

print("📊 Datos a escribir:")
display(df_agg)

# COMMAND ----------

# Escribir a nueva tabla
output_table = "dbo.ProductSummary"

df_agg.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", output_table) \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .save()

print(f"✅ Datos escritos a {output_table}")
```

---

## Ejercicio 4: Azure Service Bus (Bonus - 10 min)

Notebook: "Lab06_ServiceBus_Streaming"

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming con Azure Service Bus

# COMMAND ----------

# Parámetros Service Bus
servicebus_namespace = "<namespace>.servicebus.windows.net"
queue_name = "sales-queue"

# Connection string desde Key Vault
servicebus_conn_str = dbutils.secrets.get(scope="azure-key-vault", key="servicebus-connection-string")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer Stream desde Service Bus

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Schema del mensaje
message_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Leer stream
df_stream = spark.readStream \
    .format("servicebus") \
    .option("connectionString", servicebus_conn_str) \
    .option("queueName", queue_name) \
    .option("maxEventsPerTrigger", 100) \
    .load()

# Parsear JSON del body
df_parsed = df_stream \
    .select(
        col("body").cast("string").alias("json_data"),
        col("enqueuedTime"),
        col("sequenceNumber")
    ) \
    .select(
        from_json(col("json_data"), message_schema).alias("data"),
        col("enqueuedTime"),
        col("sequenceNumber")
    ) \
    .select("data.*", "enqueuedTime", "sequenceNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Procesar y Escribir a Delta

# COMMAND ----------

# Definir checkpointing
checkpoint_path = "/tmp/lab06/checkpoints/servicebus"
output_path = "/tmp/lab06/streaming/sales"

# Streaming query con agregación por ventana
query = df_parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("customer_id")
    ) \
    .agg(
        count("*").alias("num_transactions"),
        sum("amount").alias("total_amount")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(output_path)

print("🔄 Streaming iniciado...")
print(f"Query ID: {query.id}")
print(f"Status: {query.status}")

# COMMAND ----------

# Ver datos en tiempo real (mientras corre el stream)
# MAGIC %sql
# SELECT * FROM delta.`/tmp/lab06/streaming/sales` ORDER BY window DESC LIMIT 20

# COMMAND ----------

# Detener stream cuando termines
# query.stop()
```

---

## 🎯 Desafío Final: Pipeline de Integración Completo

Crea un pipeline end-to-end que integra múltiples servicios Azure:

### Arquitectura:

```
Azure SQL (Productos) ──┐
                        │
Event Hub (Eventos) ────┼──> Databricks ──> ADLS Gen2 (Delta Lake) ──> Cosmos DB (Serving)
                        │                                              
ADLS Gen2 (Files) ──────┘                                              
```

### Requisitos:

1. **Ingest**:
   - Lee catálogo de productos desde Azure SQL
   - Lee archivos CSV desde ADLS Gen2
   - Stream de transacciones desde Event Hub

2. **Transform**:
   - Join transacciones con catálogo de productos
   - Enriquecimiento con dimensiones
   - Cálculo de métricas (revenue, margin)
   - Data quality checks

3. **Load**:
   - Escribir a Delta Lake en ADLS Gen2 (particionado por fecha)
   - Optimizar con OPTIMIZE + Z-ORDER
   - Publicar agregados a Cosmos DB para serving layer

4. **Security**:
   - TODAS las credenciales desde Key Vault
   - Usar Service Principal para ADLS Gen2
   - Managed Identity donde sea posible

5. **Monitoring**:
   - Log métricas de cada stage
   - Alertas en caso de fallo
   - Dashboard con estadísticas

---

## ✅ Checklist de Completado

- ☐ Conectado a ADLS Gen2 con Access Key
- ☐ Conectado a ADLS Gen2 con Service Principal (OAuth)
- ☐ Creado mount point para storage
- ☐ Integrado con Cosmos DB (read/write)
- ☐ Conectado a Azure SQL con JDBC
- ☐ Implementado lectura particionada desde SQL
- ☐ (Bonus) Streaming con Service Bus o Event Hub
- ☐ Todas credenciales almacenadas en Key Vault
- ☐ Completado pipeline de integración completo

---

## 📚 Comparación de Servicios

| Servicio | Uso Principal | Latencia | Costo |
|----------|---------------|----------|-------|
| **ADLS Gen2** | Data Lake (archivos grandes) | Media | Bajo (~$0.02/GB) |
| **Cosmos DB** | Serving layer (queries rápidos) | Muy baja (<10ms) | Alto (RU-based) |
| **Azure SQL** | Datos relacionales | Baja | Medio (DTU/vCore) |
| **Event Hub** | Streaming en tiempo real | Muy baja | Medio (throughput) |
| **Service Bus** | Mensajería async | Baja | Bajo (mensajes) |

---

## 🔐 Mejores Prácticas de Seguridad

✅ **DO**:
- Usar Key Vault para TODAS las credenciales
- Preferir Service Principal sobre Access Keys
- Usar Managed Identity cuando sea posible
- Rotar secrets regularmente
- Aplicar RBAC en todos los recursos
- Usar Private Endpoints en producción

❌ **DON'T**:
- Hardcodear credenciales en notebooks
- Usar Access Keys en producción
- Compartir Service Principal secrets
- Dar permisos excesivos (seguir least privilege)

---

**🎉 ¡Felicitaciones! Has completado todos los laboratorios de Azure Databricks.**

Ahora estás listo para:
- Diseñar arquitecturas de datos en Azure
- Implementar pipelines ETL productivos
- Integrar múltiples servicios Azure
- Optimizar costos y performance
- Aplicar mejores prácticas de seguridad

## 📚 Próximos Pasos

1. Revisar [FAQ](../resources/faq.md) para dudas comunes
2. Usar [Checklist KT](../resources/checklist-kt.md) con tu partner
3. Explorar [Referencias](../resources/referencias.md) para profundizar
4. Practicar con casos de uso reales de tu organización

**¡Éxito en tu viaje con Azure Databricks! 🚀**
