# Checklist de Knowledge Transfer con Partner

## 📋 Información General

| Campo | Valor |
|-------|-------|
| **Fecha del KT** | |
| **Partner** | |
| **Contacto Principal** | |
| **Email** | |
| **Equipo Interno** | |
| **Duración Sesión** | |

---

## 1️⃣ Información del Ambiente

### Azure Subscription y Recursos

- [ ] **Subscription ID**: _______________________
- [ ] **Resource Group**: _______________________
- [ ] **Databricks Workspace Name**: _______________________
- [ ] **Region**: _______________________
- [ ] **Pricing Tier**: ☐ Standard ☐ Premium
- [ ] **URL del Workspace**: _______________________

### Accesos y Permisos

- [ ] ¿Cómo se gestiona el acceso al workspace?
  - ☐ Azure AD
  - ☐ Usuarios locales
  - ☐ SCIM provisioning
- [ ] ¿Quién tiene permisos de administrador?
- [ ] ¿Hay políticas de cluster configuradas?
- [ ] ¿Se usa Unity Catalog?

---

## 2️⃣ Arquitectura de Datos

### Fuentes de Datos

| Fuente | Tipo | Frecuencia | Ubicación | Credenciales |
|--------|------|------------|-----------|--------------|
| | ☐ Cosmos DB<br>☐ SQL DB<br>☐ Storage<br>☐ Event Hub<br>☐ Otro | ☐ Real-time<br>☐ Batch<br>☐ On-demand | | ☐ Key Vault<br>☐ Secrets<br>☐ Managed Identity |
| | | | | |
| | | | | |

### Diagrama de Arquitectura

- [ ] ¿Tienen un diagrama de arquitectura documentado?
- [ ] ¿Podemos obtener una copia?
- [ ] Ubicación del diagrama: _______________________

### Destinos de Datos

| Destino | Tipo | Propósito | Consumidores |
|---------|------|-----------|--------------|
| | ☐ Cosmos DB<br>☐ Azure SQL<br>☐ Synapse<br>☐ AI Search<br>☐ Power BI<br>☐ Otro | | |
| | | | |
| | | | |

---

## 3️⃣ Clusters

### Inventario de Clusters

| Nombre del Cluster | Tipo | Tamaño | Configuración | Costo Mensual Aprox. | Propósito |
|-------------------|------|--------|---------------|---------------------|-----------|
| | ☐ All-Purpose<br>☐ Job | Workers:<br>VM Type: | ☐ Autoscale<br>☐ Spot<br>☐ Auto-term | $ | |
| | | | | | |
| | | | | | |

### Preguntas sobre Clusters

- [ ] ¿Cuántos clusters hay actualmente activos?
- [ ] ¿Cuáles son All-Purpose vs Job clusters?
- [ ] ¿Se usan spot instances? ¿Por qué sí o no?
- [ ] ¿Hay problemas frecuentes con los clusters?
- [ ] ¿Cuál es el costo mensual aproximado de compute?
- [ ] ¿Hay políticas de cluster configuradas?
- [ ] ¿Se usa autoscaling? ¿Funciona bien?
- [ ] ¿Cuánto tiempo toma típicamente el startup de un cluster?

### Optimizaciones Identificadas

- [ ] Oportunidad: Convertir All-Purpose a Job clusters
  - Clusters candidatos: _______________________
  - Ahorro estimado: $ _______________________/mes

- [ ] Oportunidad: Habilitar spot instances
  - Clusters candidatos: _______________________
  - Ahorro estimado: $ _______________________/mes

- [ ] Oportunidad: Ajustar auto-termination
  - Clusters con idle time alto: _______________________

---

## 4️⃣ Notebooks y Jobs

### Notebooks Críticos

| Notebook | Ubicación | Propósito | Frecuencia | Owner |
|----------|-----------|-----------|------------|-------|
| | | | ☐ Diario<br>☐ Horario<br>☐ On-demand | |
| | | | | |
| | | | | |

### Preguntas sobre Notebooks

- [ ] ¿Cuántos notebooks hay en producción?
- [ ] ¿Dónde están ubicados? (rutas en Workspace)
- [ ] ¿Hay notebooks de utilidades compartidas?
- [ ] ¿Se usa control de versiones (Git)?
- [ ] ¿Cómo se documentan los notebooks?
- [ ] ¿Hay estándares de código definidos?

### Jobs en Producción

| Job Name | Schedule | Notebooks | Duración | Cluster | Alertas | Criticidad |
|----------|----------|-----------|----------|---------|---------|------------|
| | Cron:<br>______ | | ~____ min | | ☐ Email<br>☐ Teams<br>☐ Ninguna | ☐ Alta<br>☐ Media<br>☐ Baja |
| | | | | | | |
| | | | | | | |

### Preguntas sobre Jobs

- [ ] ¿Cuántos jobs hay en producción?
- [ ] ¿Cuál es el schedule de cada job?
- [ ] ¿Qué jobs son críticos para el negocio?
- [ ] ¿Hay dependencias entre jobs?
- [ ] ¿Qué pasa si falla un job?
- [ ] ¿Hay mecanismos de retry?
- [ ] ¿Quién recibe las alertas de fallos?
- [ ] ¿Cuál es el SLA de cada job?
- [ ] ¿Se monitorea la duración de los jobs?
- [ ] ¿Ha habido jobs que fallen frecuentemente?

---

## 5️⃣ Flujo de Datos

### Patrón de Arquitectura

- [ ] ¿Usan Medallion Architecture (Bronze/Silver/Gold)?
- [ ] ¿Dónde se almacenan cada capa?
  - Bronze: _______________________
  - Silver: _______________________
  - Gold: _______________________

### Delta Lake

- [ ] ¿Se usa Delta Lake?
- [ ] ¿Dónde están las tablas Delta?
- [ ] ¿Se hacen OPTIMIZE regularmente?
- [ ] ¿Se hace Z-ORDERING?
- [ ] ¿Se hace VACUUM?
- [ ] ¿Hay problemas con archivos pequeños?

### Transformaciones

- [ ] ¿Qué tipo de transformaciones se realizan?
  - ☐ Limpieza de datos
  - ☐ Agregaciones
  - ☐ Joins complejos
  - ☐ Machine Learning
  - ☐ Streaming

- [ ] ¿Hay lógica de negocio compleja?
- [ ] ¿Cómo se valida la calidad de los datos?
- [ ] ¿Qué hacer cuando hay datos malos?

---

## 6️⃣ Integración con Servicios Azure

### Cosmos DB

- [ ] ¿Se usa Cosmos DB?
- [ ] ¿Para qué? ☐ Lectura ☐ Escritura ☐ Ambos
- [ ] Cuenta de Cosmos: _______________________
- [ ] Databases: _______________________
- [ ] Containers: _______________________
- [ ] ¿Hay problemas de throttling?
- [ ] ¿Cómo se gestionan los RUs?

### Azure Storage (ADLS Gen2)

- [ ] Storage Account(s): _______________________
- [ ] Containers usados: _______________________
- [ ] ¿Cómo se autentica? ☐ Key ☐ SAS ☐ Managed Identity
- [ ] ¿Hay mounts configurados?
- [ ] ¿Dónde se documentan las rutas?

### Azure AI Search

- [ ] ¿Se usa AI Search?
- [ ] Search Service name: _______________________
- [ ] Índices: _______________________
- [ ] ¿Cómo se actualiza el índice?

### Otros Servicios

- [ ] **Azure SQL Database**
  - Servidor: _______________________
  - Bases de datos: _______________________

- [ ] **Azure Synapse**
  - Workspace: _______________________
  - SQL Pools: _______________________

- [ ] **Event Hubs**
  - Namespace: _______________________
  - Event Hubs: _______________________

- [ ] **Power BI**
  - Workspace: _______________________
  - Reports conectados: _______________________

---

## 7️⃣ Seguridad y Secrets

### Key Vault

- [ ] Key Vault name: _______________________
- [ ] ¿Qué secrets se almacenan ahí?
- [ ] ¿Cómo se configura el Databricks Secret Scope?
- [ ] Scope name: _______________________

### Autenticación

- [ ] ¿Se usa Managed Identity?
- [ ] ¿Dónde se configura?
- [ ] ¿Se usa Service Principal?
  - Client ID: _______________________
  - ¿Dónde está el secret?

### Networking

- [ ] ¿El workspace está en VNet?
- [ ] ¿Se usa Private Link?
- [ ] ¿Hay restricciones de firewall?
- [ ] ¿Se puede acceder desde internet?

---

## 8️⃣ Monitoreo y Alertas

### Monitoreo

- [ ] ¿Cómo se monitorean los jobs?
  - ☐ Databricks UI
  - ☐ Azure Monitor
  - ☐ Application Insights
  - ☐ Otro: _______________________

- [ ] ¿Se revisan logs regularmente?
- [ ] ¿Dónde se almacenan los logs?
- [ ] ¿Hay dashboards de monitoreo?

### Alertas

- [ ] ¿Qué alertas están configuradas?
  - ☐ Job failures
  - ☐ Cluster start failures
  - ☐ High costs
  - ☐ Data quality issues
  - ☐ Otro: _______________________

- [ ] ¿A quién se envían las alertas?
- [ ] ¿Cuál es el proceso de escalación?

### Costos

- [ ] ¿Cómo se monitorean los costos?
- [ ] ¿Hay presupuesto definido?
- [ ] Presupuesto mensual: $ _______________________
- [ ] ¿Se revisan costos regularmente?
- [ ] ¿Hay alertas de costos configuradas?

---

## 9️⃣ Documentación

### Documentación Existente

- [ ] ¿Dónde está la documentación?
  - ☐ Confluence
  - ☐ SharePoint
  - ☐ Wiki
  - ☐ README files
  - ☐ No hay documentación formal

- [ ] ¿Está actualizada?
- [ ] ¿Incluye diagramas?
- [ ] ¿Hay runbooks para troubleshooting?

### Documentación a Solicitar

- [ ] Diagrama de arquitectura
- [ ] Flujo de datos end-to-end
- [ ] Lista de jobs y su propósito
- [ ] Credenciales y accesos (securely)
- [ ] Procedimientos de operación
- [ ] Contactos de escalación
- [ ] Documentación de APIs/endpoints (si aplica)

---

## 🔟 Troubleshooting y Soporte

### Problemas Conocidos

| Problema | Frecuencia | Impacto | Workaround | Owner |
|----------|------------|---------|------------|-------|
| | ☐ Diario<br>☐ Semanal<br>☐ Mensual<br>☐ Raro | ☐ Alto<br>☐ Medio<br>☐ Bajo | | |
| | | | | |
| | | | | |

### Preguntas de Troubleshooting

- [ ] ¿Qué problemas han experimentado?
- [ ] ¿Cuáles son los más frecuentes?
- [ ] ¿Cómo se resuelven típicamente?
- [ ] ¿Hay tickets/issues documentados?
- [ ] ¿A quién contactar para soporte?

### Contactos de Escalación

| Rol | Nombre | Email | Teléfono | Disponibilidad |
|-----|--------|-------|----------|----------------|
| **Contacto Principal** | | | | |
| **Backup** | | | | |
| **Databricks Expert** | | | | |
| **Azure Admin** | | | | |
| **On-call** | | | | |

---

## 1️⃣1️⃣ Plan de Transición

### Actividades de Transición

| Actividad | Responsable | Fecha Límite | Status |
|-----------|-------------|--------------|--------|
| Transferir accesos | | | ☐ Pendiente<br>☐ En proceso<br>☐ Completado |
| Documentar runbooks | | | ☐ Pendiente<br>☐ En proceso<br>☐ Completado |
| Actualizar contactos en alertas | | | ☐ Pendiente<br>☐ En proceso<br>☐ Completado |
| Sesión de shadow (observar) | | | ☐ Pendiente<br>☐ En proceso<br>☐ Completado |
| Sesión hands-on (hacer) | | | ☐ Pendiente<br>☐ En proceso<br>☐ Completado |
| Verificar backups | | | ☐ Pendiente<br>☐ En proceso<br>☐ Completado |

### Período de Soporte

- [ ] ¿Habrá período de soporte del partner?
- [ ] Duración: _______ días/semanas
- [ ] Disponibilidad: _______________________
- [ ] Canales de comunicación: _______________________

---

## 1️⃣2️⃣ Preguntas Adicionales

### Negocio

- [ ] ¿Cuál es el objetivo de negocio de esta solución?
- [ ] ¿Quiénes son los stakeholders?
- [ ] ¿Qué métricas de negocio se generan?
- [ ] ¿Cómo se consume la información?

### Técnico

- [ ] ¿Hay planes de mejoras futuras?
- [ ] ¿Hay deuda técnica conocida?
- [ ] ¿Qué se podría optimizar?
- [ ] ¿Hay limitaciones actuales?

### Operacional

- [ ] ¿Cuál es el horario de operación?
- [ ] ¿Hay ventanas de mantenimiento?
- [ ] ¿Cómo se manejan cambios en producción?
- [ ] ¿Hay proceso de change management?

---

## ✅ Checklist Final

### Antes de Cerrar el KT

- [ ] Todos los accesos verificados
- [ ] Documentación recibida y revisada
- [ ] Diagramas de arquitectura obtenidos
- [ ] Contactos de escalación documentados
- [ ] Al menos 1 ejecución de job observada
- [ ] Troubleshooting básico practicado
- [ ] Preguntas críticas respondidas
- [ ] Plan de transición acordado
- [ ] Fecha de próxima sesión (si aplica): _______________________

### Post-KT

- [ ] Actualizar documentación interna
- [ ] Compartir conocimiento con equipo
- [ ] Identificar mejoras u optimizaciones
- [ ] Programar revisión en 30 días

---

## 📝 Notas Adicionales

```
(Espacio para notas adicionales durante el KT)





















```

---

## 📋 Anexos

### Anexo A: Credenciales y Accesos

*Nota: Gestionar de forma segura, no compartir en texto plano*

| Recurso | Tipo | Ubicación | Acceso |
|---------|------|-----------|--------|
| | | Key Vault secret: | |
| | | Key Vault secret: | |

### Anexo B: URLs y Enlaces Importantes

| Recurso | URL |
|---------|-----|
| Databricks Workspace | |
| Azure Portal - Resource Group | |
| Documentación | |
| Repositorio Git (si aplica) | |
| Dashboard de Monitoreo | |

---

**Fecha de creación del checklist**: _______________________  
**Última actualización**: _______________________  
**Próxima revisión**: _______________________
