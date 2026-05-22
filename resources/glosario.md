# Glosario de Términos - Azure Databricks

## A

### **ACID**
Atomicity, Consistency, Isolation, Durability. Propiedades que garantizan la confiabilidad de transacciones en bases de datos. Delta Lake proporciona ACID compliance.

### **ADLS Gen2**
Azure Data Lake Storage Generation 2. Servicio de almacenamiento de Azure optimizado para big data analytics, combina las características de Azure Blob Storage y Azure Data Lake Storage Gen1.

### **All-Purpose Cluster**
Cluster interactivo usado para desarrollo y exploración. Más caro que Job clusters (~$0.55/DBU vs $0.22/DBU). Puede ser compartido por múltiples usuarios.

### **Apache Spark**
Motor de procesamiento distribuido open-source para análisis de big data. Fundamento de Azure Databricks.

### **Auto-termination**
Característica que automáticamente apaga un cluster después de un período de inactividad configurado (ej: 30 minutos).

### **Autoscaling**
Capacidad de un cluster de aumentar o reducir automáticamente el número de workers basado en la carga de trabajo.

### **Azure Databricks**
Plataforma de análisis de datos basada en Apache Spark, optimizada para Azure Cloud.

---

## B

### **Batch Processing**
Procesamiento de grandes volúmenes de datos recopilados durante un período de tiempo (ej: diario, horario).

### **Broadcast Join**
Optimización de Spark que envía una tabla pequeña a todos los nodos del cluster para hacer joins más eficientes. Usado cuando una tabla es mucho más pequeña que la otra.

### **Bronze Layer**
Primera capa en Medallion Architecture. Contiene datos raw (sin transformar) tal como llegan de las fuentes.

---

## C

### **Cache**
Almacenamiento temporal de un DataFrame en memoria para reutilización rápida. Útil cuando el DataFrame se usa múltiples veces.

### **Cluster**
Conjunto de máquinas virtuales que trabajan juntas para procesar datos. Consiste en un driver node y worker nodes.

### **Cluster Pool**
Conjunto de VMs inactivas que pueden ser usadas rápidamente para crear clusters, reduciendo el tiempo de startup de ~5 minutos a ~30 segundos.

### **Cluster Policy**
Reglas definidas por administradores que limitan las opciones de configuración de clusters (ej: tipos de VM permitidos, tamaño máximo).

### **Cosmos DB**
Base de datos NoSQL de Azure con latencia ultra-baja, usada frecuentemente como serving layer en arquitecturas Databricks.

---

## D

### **Databricks Runtime**
Versión de Apache Spark pre-configurada y optimizada por Databricks. Incluye librerías adicionales y optimizaciones.

### **DataFrame**
Estructura de datos distribuida organizada en columnas. Abstracción principal en PySpark para trabajar con datos.

### **Data Lake**
Repositorio centralizado que almacena grandes cantidades de datos en su formato nativo (structured, semi-structured, unstructured).

### **DBU (Databricks Unit)**
Unidad de medida de procesamiento en Databricks. Representa una combinación de compute, memoria y I/O. Base para facturación.

### **Delta Lake**
Capa de almacenamiento open-source que proporciona ACID transactions, time travel y optimizaciones sobre data lakes.

### **Driver Node**
Nodo principal del cluster que coordina el trabajo y mantiene el contexto de Spark. Ejecuta el código del notebook.

---

## E

### **ETL (Extract, Transform, Load)**
Proceso de extraer datos de fuentes, transformarlos y cargarlos en destinos para análisis.

### **Event Hub**
Servicio de Azure para ingesta de streaming de eventos a gran escala.

---

## G

### **Gold Layer**
Tercera capa en Medallion Architecture. Contiene datos agregados y optimizados para consumo por usuarios de negocio y aplicaciones.

---

## J

### **Job**
Ejecución programada o manual de uno o más notebooks o scripts. Típicamente corre en Job Clusters.

### **Job Cluster**
Cluster creado automáticamente para un job específico y destruido al terminar. ~50% más barato que All-Purpose clusters.

---

## K

### **Key Vault**
Servicio de Azure para almacenar y acceder secrets (passwords, API keys, certificates) de forma segura.

---

## L

### **Lakehouse**
Arquitectura que combina lo mejor de data warehouses y data lakes: estructura y gestión de warehouses con flexibilidad y escala de lakes.

### **Lambda Architecture**
Patrón arquitectónico que procesa datos a través de dos paths: batch layer (procesamiento completo) y speed layer (procesamiento en tiempo real).

---

## M

### **Managed Identity**
Identidad en Azure AD que permite a servicios autenticarse sin almacenar credenciales en código.

### **Medallion Architecture**
Patrón de diseño de data lakes con tres capas: Bronze (raw), Silver (cleaned), Gold (aggregated).

### **MERGE**
Operación de Delta Lake que permite hacer upserts (INSERT + UPDATE) basados en condiciones.

### **Mount**
Configuración que permite acceder a Azure Storage como si fuera un directorio local en Databricks (`/mnt/`).

---

## N

### **Notebook**
Documento interactivo que combina código ejecutable, visualizaciones y texto Markdown. Similar a Jupyter Notebooks pero con características adicionales.

---

## O

### **OPTIMIZE**
Comando de Delta Lake que consolida archivos pequeños en archivos más grandes para mejorar rendimiento de lectura.

---

## P

### **Parquet**
Formato de archivo columnar optimizado para big data analytics. Formato base de Delta Lake.

### **Partition**
División de datos en segmentos más pequeños, ya sea en memoria (Spark partitions) o en disco (por columnas como fecha).

### **Photon**
Motor de ejecución nativo de Databricks escrito en C++, más rápido que el motor JVM estándar de Spark.

### **Pool** (ver Cluster Pool)

### **Premium Tier**
Nivel de pricing de Databricks que incluye características avanzadas como RBAC, audit logs y conditional access.

### **PySpark**
API de Python para Apache Spark. Lenguaje más común en Databricks notebooks.

---

## R

### **RDD (Resilient Distributed Dataset)**
Abstracción de bajo nivel en Spark para datos distribuidos. Reemplazado en gran parte por DataFrames.

### **Repo**
Integración con Git (Azure DevOps, GitHub) para version control de notebooks.

---

## S

### **Scala**
Lenguaje de programación en el que está escrito Spark. Puede usarse en Databricks notebooks.

### **Secret Scope**
Contenedor para secrets en Databricks, típicamente backed por Azure Key Vault.

### **Service Principal**
Identidad de aplicación en Azure AD usada para autenticación de servicio-a-servicio.

### **Shuffle**
Operación en Spark que redistribuye datos entre particiones. Costosa en términos de I/O y red.

### **Silver Layer**
Segunda capa en Medallion Architecture. Contiene datos limpiados, validados y conformados.

### **Spark SQL**
Módulo de Spark para trabajar con datos estructurados usando SQL.

### **Spot Instance**
VM de Azure con hasta 80% de descuento, pero puede ser interrumpida si Azure necesita la capacidad.

### **Standard Tier**
Nivel base de pricing de Databricks con características esenciales.

### **Streaming**
Procesamiento continuo de datos en tiempo real a medida que llegan.

---

## T

### **Table**
Vista lógica de datos en Databricks, puede ser managed (Databricks gestiona datos) o external (datos en ubicación externa).

### **Task**
Unidad mínima de ejecución en Spark. Múltiples tasks se ejecutan en paralelo en diferentes workers.

### **Time Travel**
Característica de Delta Lake que permite consultar versiones anteriores de datos.

---

## U

### **Unity Catalog**
Sistema de gobierno de datos unificado en Databricks para gestionar acceso y descubrimiento de datos.

### **Upsert**
Operación que hace UPDATE si el registro existe, INSERT si no existe. Implementado con MERGE en Delta Lake.

---

## V

### **VACUUM**
Comando de Delta Lake que elimina archivos viejos que ya no son necesarios (después de retención configurada, ej: 7 días).

### **VNet (Virtual Network)**
Red virtual privada en Azure. Databricks puede deployarse dentro de una VNet para mayor control de red.

---

## W

### **Widget**
Control de UI en notebooks que permite pasar parámetros de forma interactiva.

### **Worker Node**
Nodos en un cluster que ejecutan las tasks asignadas por el driver. Un cluster puede tener 0 a cientos de workers.

### **Workspace**
Entorno colaborativo en Databricks donde se organizan notebooks, carpetas, clusters, jobs, etc.

---

## Z

### **Z-Ordering**
Técnica de optimización de Delta Lake que co-localiza columnas relacionadas en archivos para mejorar rendimiento de queries.

---

## Símbolos Especiales

### **%md**
Magic command para escribir celdas en Markdown en notebooks.

### **%python / %sql / %scala / %r**
Magic commands para cambiar el lenguaje de una celda en notebooks multi-lenguaje.

### **%run**
Magic command para ejecutar otro notebook desde el actual.

### **%sh**
Magic command para ejecutar comandos shell en el driver node.

---

## Unidades de Medida

### **DBU (Databricks Unit)**
Unidad de procesamiento y facturación en Databricks.

### **GB/TB/PB**
Gigabyte, Terabyte, Petabyte. Unidades de almacenamiento de datos.

### **RU (Request Unit)**
Unidad de throughput en Cosmos DB.

---

## Acrónimos Comunes

| Acrónimo | Significado | Contexto |
|----------|-------------|----------|
| **ACID** | Atomicity, Consistency, Isolation, Durability | Transacciones |
| **ADLS** | Azure Data Lake Storage | Storage |
| **AQE** | Adaptive Query Execution | Spark optimization |
| **DAG** | Directed Acyclic Graph | Spark execution plan |
| **ETL** | Extract, Transform, Load | Data pipelines |
| **JDBC** | Java Database Connectivity | Database connections |
| **JSON** | JavaScript Object Notation | Data format |
| **ML** | Machine Learning | Analytics |
| **ODBC** | Open Database Connectivity | Database connections |
| **RBAC** | Role-Based Access Control | Security |
| **SAS** | Shared Access Signature | Azure authentication |
| **SQL** | Structured Query Language | Queries |
| **VM** | Virtual Machine | Compute |

---

## Referencias

Para más términos, consulta:
- [Documentación oficial de Databricks](https://docs.databricks.com/glossary.html)
- [Spark Glossary](https://spark.apache.org/docs/latest/api/python/getting_started/glossary.html)

---

**Última actualización**: 2026-05-22
