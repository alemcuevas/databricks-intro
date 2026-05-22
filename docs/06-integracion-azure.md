# Integración con Servicios Azure

## 📚 Índice

1. [Azure Storage: Configuración y Opciones](#azure-storage-configuración-y-opciones)
2. [Azure Key Vault: Gestión de Secrets](#azure-key-vault-gestión-de-secrets)
3. [Azure DevOps y CI/CD](#azure-devops-y-cicd)
4. [Networking: VNet y Private Endpoints](#networking-vnet-y-private-endpoints)
5. [Azure SQL Database](#azure-sql-database)
6. [Azure Service Bus](#azure-service-bus)
7. [Azure Monitor y Application Insights](#azure-monitor-y-application-insights)

---

## 1. Azure Storage: Configuración y Opciones

### 1.1 Métodos de Autenticación

#### Opción 1: Access Key (Más Simple, Menos Seguro)

```python
# Configurar access key en session
storage_account = "mystorageaccount"
access_key = dbutils.secrets.get("keyvault", "storage-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

# Leer datos
df = spark.read.parquet(f"abfss://container@{storage_account}.dfs.core.windows.net/path/")
```

**Ventajas**: Fácil de configurar  
**Desventajas**: Access key tiene permisos completos, riesgo de seguridad

---

#### Opción 2: Service Principal (Recomendado para Producción)

**Paso 1: Crear Service Principal en Azure AD**
```bash
# Azure CLI
az ad sp create-for-rbac \
    --name "databricks-sp" \
    --role "Storage Blob Data Contributor" \
    --scopes /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage>
```

**Paso 2: Configurar en Databricks**
```python
# Secrets en Key Vault
tenant_id = dbutils.secrets.get("keyvault", "tenant-id")
client_id = dbutils.secrets.get("keyvault", "sp-client-id")
client_secret = dbutils.secrets.get("keyvault", "sp-client-secret")
storage_account = "mystorageaccount"

# Configurar OAuth
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Leer datos
df = spark.read.parquet(f"abfss://container@{storage_account}.dfs.core.windows.net/data/")
```

---

#### Opción 3: Managed Identity (Más Seguro, Sin Secrets)

**Paso 1: Habilitar Managed Identity para Databricks**
```bash
# En Azure Portal:
# Databricks workspace → Settings → Managed Identity → Enable
```

**Paso 2: Asignar permisos RBAC**
```bash
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee <managed-identity-principal-id> \
    --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage>
```

**Paso 3: Configurar en cluster**
```json
{
  "spark_conf": {
    "spark.hadoop.fs.azure.account.auth.type": "ManagedIdentity",
    "spark.hadoop.fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
  }
}
```

**Paso 4: Usar en notebooks**
```python
# ¡No se necesitan secrets!
df = spark.read.parquet("abfss://container@mystorageaccount.dfs.core.windows.net/data/")
```

---

### 1.2 Montaje de Storage (Mount)

**Crear mount point**:

```python
def mount_storage(mount_name, container, storage_account, use_managed_identity=True):
    """
    Monta un container de ADLS Gen2 en /mnt/<mount_name>
    """
    mount_point = f"/mnt/{mount_name}"
    
    # Verificar si ya está montado
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"✅ {mount_point} ya está montado")
        return
    
    # Configurar autenticación
    if use_managed_identity:
        configs = {
            "fs.azure.account.auth.type": "ManagedIdentity",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        }
    else:
        # Service Principal
        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": dbutils.secrets.get("keyvault", "sp-client-id"),
            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("keyvault", "sp-secret"),
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get('keyvault', 'tenant-id')}/oauth2/token"
        }
    
    # Montar
    dbutils.fs.mount(
        source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    
    print(f"✅ Montado {container} en {mount_point}")

# Uso
mount_storage("datalake", "raw-data", "mydatalake", use_managed_identity=True)

# Ahora puedes usar paths simples
df = spark.read.parquet("/mnt/datalake/sales/2026-05-22/")
```

**Desmontar**:
```python
dbutils.fs.unmount("/mnt/datalake")
```

---

## 2. Azure Key Vault: Gestión de Secrets

### 2.1 Crear Secret Scope

**Opción 1: Azure Key Vault-backed (Recomendado)**

```bash
# Obtener info de Key Vault
az keyvault show --name <keyvault-name> --query "{id:id, uri:properties.vaultUri}"

# Output:
# {
#   "id": "/subscriptions/.../resourceGroups/.../providers/Microsoft.KeyVault/vaults/<keyvault>",
#   "uri": "https://<keyvault>.vault.azure.net/"
# }
```

**Crear scope en Databricks**:
```
1. Ir a: https://<databricks-instance>#secrets/createScope
2. Scope Name: "keyvault"
3. Manage Principal: "All Users" (o "Creator")
4. DNS Name: https://<keyvault>.vault.azure.net/
5. Resource ID: /subscriptions/.../vaults/<keyvault>
```

**O vía CLI**:
```bash
databricks secrets create-scope \
    --scope keyvault \
    --scope-backend-type AZURE_KEYVAULT \
    --resource-id "/subscriptions/.../vaults/<keyvault>" \
    --dns-name "https://<keyvault>.vault.azure.net/"
```

### 2.2 Usar Secrets en Notebooks

```python
# Leer secret
storage_key = dbutils.secrets.get("keyvault", "storage-account-key")

# ⚠️ NO imprimas secrets directamente
# ❌ print(storage_key)  # Se mostrará como [REDACTED]

# ✅ Usar en configs
spark.conf.set("fs.azure.account.key.mystorageaccount.dfs.core.windows.net", storage_key)
```

### 2.3 Listar Secrets

```python
# Listar scopes
dbutils.secrets.listScopes()
# Output: [SecretScope(name='keyvault')]

# Listar secrets en un scope
dbutils.secrets.list("keyvault")
# Output: [SecretMetadata(key='storage-account-key'), ...]
```

---

## 3. Azure DevOps y CI/CD

### 3.1 Conectar Databricks con Azure DevOps

**Configuración de Repos en Databricks**:

```
1. Workspace → Repos → Add Repo
2. Git provider: Azure DevOps Services
3. Git URL: https://dev.azure.com/<org>/<project>/_git/<repo>
4. Autenticar con Personal Access Token (PAT)
```

### 3.2 Pipeline CI/CD Completo

**azure-pipelines.yml**:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - notebooks/*
      - jobs/*

variables:
  databricksUrl: 'https://adb-1234567890.azuredatabricks.net'
  # databricksToken stored in Azure DevOps Library

stages:
  - stage: Build
    displayName: 'Build and Test'
    jobs:
      - job: Tests
        displayName: 'Run Unit Tests'
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.10'
          
          - script: |
              pip install pytest pyspark==3.4.0 databricks-sdk
              pytest tests/ --junitxml=test-results.xml
            displayName: 'Run pytest'
          
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'test-results.xml'
              testRunTitle: 'Python Unit Tests'

  - stage: DeployDev
    displayName: 'Deploy to Development'
    dependsOn: Build
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/develop'))
    jobs:
      - deployment: DeployNotebooksDev
        environment: 'Databricks-Dev'
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                
                - task: Bash@3
                  displayName: 'Install Databricks CLI'
                  inputs:
                    targetType: 'inline'
                    script: |
                      pip install databricks-cli
                      databricks configure --token <<EOF
                      $(databricksUrl)
                      $(databricksToken)
                      EOF
                
                - task: Bash@3
                  displayName: 'Deploy Notebooks'
                  inputs:
                    targetType: 'inline'
                    script: |
                      databricks workspace import_dir \
                        notebooks/ \
                        /Shared/Development/ \
                        --overwrite

  - stage: DeployProd
    displayName: 'Deploy to Production'
    dependsOn: Build
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployNotebooksProd
        environment: 'Databricks-Prod'  # Requiere aprobación manual
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                
                - task: Bash@3
                  displayName: 'Deploy to Production'
                  inputs:
                    targetType: 'inline'
                    script: |
                      pip install databricks-cli
                      databricks configure --token <<EOF
                      $(databricksUrl)
                      $(databricksTokenProd)
                      EOF
                      
                      # Deploy notebooks
                      databricks workspace import_dir \
                        notebooks/ \
                        /Shared/Production/ \
                        --overwrite
                      
                      # Update job definition
                      databricks jobs reset --job-id $(jobId) --json-file jobs/etl-pipeline.json
```

---

## 4. Networking: VNet y Private Endpoints

### 4.1 VNet Injection

**Beneficios**:
- ✅ Clusters en tu propia VNet
- ✅ Control completo de networking
- ✅ Acceso a recursos privados (Azure SQL con Private Link)
- ✅ Integración con on-premises vía VPN/ExpressRoute

**Configuración**:

```bash
# Crear subnets para Databricks
az network vnet subnet create \
    --resource-group myRG \
    --vnet-name myVNet \
    --name databricks-public \
    --address-prefixes 10.0.1.0/24 \
    --delegations Microsoft.Databricks/workspaces

az network vnet subnet create \
    --resource-group myRG \
    --vnet-name myVNet \
    --name databricks-private \
    --address-prefixes 10.0.2.0/24 \
    --delegations Microsoft.Databricks/workspaces

# Crear workspace con VNet injection
az databricks workspace create \
    --resource-group myRG \
    --name myWorkspace \
    --location eastus \
    --sku premium \
    --vnet myVNet \
    --public-subnet-name databricks-public \
    --private-subnet-name databricks-private
```

### 4.2 Private Link para Storage

**Crear Private Endpoint**:

```bash
# Deshabilitar network policies
az network vnet subnet update \
    --resource-group myRG \
    --vnet-name myVNet \
    --name databricks-private \
    --disable-private-endpoint-network-policies true

# Crear private endpoint para Storage
az network private-endpoint create \
    --resource-group myRG \
    --name storage-pe \
    --vnet-name myVNet \
    --subnet databricks-private \
    --private-connection-resource-id $(az storage account show -n mystorage --query id -o tsv) \
    --group-id blob \
    --connection-name storage-connection

# Configurar DNS
az network private-dns zone create \
    --resource-group myRG \
    --name privatelink.blob.core.windows.net

az network private-dns link vnet create \
    --resource-group myRG \
    --zone-name privatelink.blob.core.windows.net \
    --name storage-dns-link \
    --virtual-network myVNet \
    --registration-enabled false
```

---

## 5. Azure SQL Database

### 5.1 Leer desde Azure SQL

```python
# Configuración
jdbc_hostname = "myserver.database.windows.net"
jdbc_port = 1433
jdbc_database = "mydb"
jdbc_username = dbutils.secrets.get("keyvault", "sql-username")
jdbc_password = dbutils.secrets.get("keyvault", "sql-password")

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

connection_properties = {
    "user": jdbc_username,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Leer tabla completa
df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.Customers",
    properties=connection_properties
)

# Leer con query
query = """
(SELECT customer_id, name, total_purchases
 FROM dbo.Customers
 WHERE country = 'USA') AS customers_query
"""
df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=connection_properties
)

# Leer con particiones (para grandes volúmenes)
df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.LargeTable",
    column="customer_id",  # Columna numérica para particionar
    lowerBound=1,
    upperBound=1000000,
    numPartitions=10,
    properties=connection_properties
)
```

### 5.2 Escribir a Azure SQL

```python
# Escribir DataFrame
df.write.jdbc(
    url=jdbc_url,
    table="dbo.AnalyticsResults",
    mode="append",  # or "overwrite", "ignore", "error"
    properties=connection_properties
)

# Upsert (MERGE) manual
def upsert_to_sql(df, table_name, key_columns):
    """
    Implementa upsert (merge) a SQL Server
    """
    # Crear tabla temporal
    temp_table = f"#temp_{table_name}"
    df.write.jdbc(
        url=jdbc_url,
        table=temp_table,
        mode="overwrite",
        properties=connection_properties
    )
    
    # Ejecutar MERGE
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    USING {temp_table} AS source
    ON {' AND '.join([f'target.{col} = source.{col}' for col in key_columns])}
    WHEN MATCHED THEN
        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in df.columns if col not in key_columns])}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(df.columns)})
        VALUES ({', '.join([f'source.{col}' for col in df.columns])});
    """
    
    # Ejecutar vía JDBC
    # (requiere configuración adicional de ejecución)
```

---

## 6. Azure Service Bus

### 6.1 Leer Mensajes con Streaming

```python
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

# Configuración
connection_string = dbutils.secrets.get("keyvault", "servicebus-connection-string")
queue_name = "orders-queue"

# Configurar Structured Streaming desde Service Bus
df_stream = spark.readStream \
    .format("com.microsoft.azure.servicebus.spark.structured.streaming.ServiceBusSource") \
    .option("connectionString", connection_string) \
    .option("queueName", queue_name) \
    .option("maxBatchSize", 1000) \
    .load()

# Procesar mensajes
df_processed = df_stream \
    .select(
        col("body").cast("string").alias("message"),
        col("enqueuedTime"),
        col("messageId")
    ) \
    .withColumn("data", from_json(col("message"), schema)) \
    .select("data.*", "enqueuedTime", "messageId")

# Escribir a Delta Lake
query = df_processed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/servicebus/") \
    .start("/mnt/delta/orders/")

query.awaitTermination()
```

---

## 7. Azure Monitor y Application Insights

### 7.1 Enviar Métricas Personalizadas

```python
import requests
import json
from datetime import datetime

def send_custom_metric(metric_name, value, dimensions=None):
    """
    Envía métrica personalizada a Application Insights
    """
    instrumentation_key = dbutils.secrets.get("keyvault", "appinsights-key")
    
    telemetry = {
        "name": f"Microsoft.ApplicationInsights.{instrumentation_key}.Metric",
        "time": datetime.utcnow().isoformat() + "Z",
        "iKey": instrumentation_key,
        "data": {
            "baseType": "MetricData",
            "baseData": {
                "metrics": [{
                    "name": metric_name,
                    "value": value,
                    "count": 1
                }],
                "properties": dimensions or {}
            }
        }
    }
    
    response = requests.post(
        "https://dc.services.visualstudio.com/v2/track",
        json=telemetry,
        headers={"Content-Type": "application/json"}
    )
    
    return response.status_code == 200

# Uso
send_custom_metric(
    "ETL.RecordsProcessed",
    10000,
    dimensions={
        "Pipeline": "Daily_Sales",
        "Environment": "Production"
    }
)
```

### 7.2 Integrar con Log Analytics

```python
# Enviar logs estructurados
import hashlib
import hmac
import base64

def send_to_log_analytics(workspace_id, shared_key, log_type, json_data):
    """
    Envía logs personalizados a Log Analytics
    """
    body = json.dumps(json_data)
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    
    # Construir signature
    date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(body)
    
    string_to_sign = f"{method}\n{content_length}\n{content_type}\nx-ms-date:{date}\n{resource}"
    
    bytes_to_hash = bytes(string_to_sign, encoding="utf-8")  
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
    authorization = f"SharedKey {workspace_id}:{encoded_hash}"
    
    # Enviar
    url = f"https://{workspace_id}.ods.opinsights.azure.com{resource}?api-version=2016-04-01"
    headers = {
        'content-type': content_type,
        'Authorization': authorization,
        'Log-Type': log_type,
        'x-ms-date': date
    }
    
    response = requests.post(url, data=body, headers=headers)
    return response.status_code

# Uso
workspace_id = dbutils.secrets.get("keyvault", "log-analytics-workspace-id")
shared_key = dbutils.secrets.get("keyvault", "log-analytics-key")

log_data = [{
    "PipelineName": "ETL_Sales",
    "RecordsProcessed": 10000,
    "DurationSeconds": 120,
    "Status": "SUCCESS",
    "Timestamp": datetime.utcnow().isoformat()
}]

send_to_log_analytics(workspace_id, shared_key, "DatabricksETL", log_data)
```

---

## 📚 Recursos Adicionales

- [Azure Databricks Networking](https://docs.microsoft.com/azure/databricks/administration-guide/cloud-configurations/azure/vnet-inject)
- [Key Vault Best Practices](https://docs.microsoft.com/azure/key-vault/general/best-practices)
- [Azure DevOps Integration](https://docs.microsoft.com/azure/databricks/dev-tools/ci-cd/ci-cd-azure-devops)

---

**Siguiente**: [Laboratorio 1 - Configuración Inicial](../labs/lab-01-configuracion-inicial.md)
