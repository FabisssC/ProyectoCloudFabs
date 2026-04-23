# RUNBOOK

## Objetivo

Documentar como operar localmente el pipeline medallion y como usar la referencia de publicacion en GCP.

## Preparacion local

### 1. Crear y activar entorno virtual

PowerShell:

```powershell
cd Proyecto
python -m venv .venv
.venv_proy\Scripts\Activate.ps1
```
```
 .\venv_proy\Scripts\Activate.ps1    
```
### 2. Entrar a carpeta del Proyecto 
```
cd Proyecto
```

### 3. Instalar dependencias

```powershell
pip install -r requirements.txt -r requirements-dev.txt
```

### 4. Configurar variables de entorno
Crear una copia del archivo .env.example a un archivo .env con las variables propias.
```powershell
Copy-Item .env.example .env
```

Ajustar al menos:

- `INPUT_TRANSACTIONS_PATH`
- `INPUT_MERCHANTS_PATH`
- `FX_API_URL`
- `GCS_BUCKET`
- `GCP_PROJECT_ID`
- `GCP_REGION`

## Autenticacion GCP en desarrollo con ADC

Paso a paso recomendado para entorno local:

1. instalar y abrir Google Cloud SDK
2. autenticar tu usuario:

```powershell
gcloud auth login
```

3. fijar el proyecto:

```powershell
gcloud config set project arboreal-logic-493416-i6
```

4. generar Application Default Credentials:

```powershell
gcloud auth application-default login
```

5. asignar el proyecto de cuota para ADC:

```powershell
gcloud auth application-default set-quota-project arboreal-logic-493416-i6
```

6. validar autenticacion:

```powershell
gcloud auth application-default print-access-token
```

Con este flujo no es necesario guardar `GOOGLE_APPLICATION_CREDENTIALS` en `.env`.

## Alternativa con service account JSON

Si quieres una version mas operativa y no interactiva:

1. crear o usar un proyecto dedicado en Google Cloud
2. crear un bucket GCS especifico para el pipeline
3. crear una cuenta de servicio dedicada
4. asignar permisos minimos sobre GCS y BigQuery
5. crear una clave JSON desde IAM and Admin > Service Accounts > Keys > Add key > Create new key > JSON
6. descargar el archivo y guardarlo fuera del repositorio o en una carpeta local ignorada por Git
7. configurar:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\ruta\segura\sa-medallion-pipeline.json"
```

o en `.env`:

```env
# Opcional solo si usas service account JSON
GOOGLE_APPLICATION_CREDENTIALS=C:/ruta/segura/sa-medallion-pipeline.json
```

## Ejecucion del pipeline

### 1. Bronze

```powershell
python -m src.pipeline.orchestration.run_local_ingestion
```

### 2. Silver

```powershell
python -m src.pipeline.transform.build_silver
```

### 3. Gold full

```powershell
python -m src.pipeline.gold.build_gold --mode full
```

### 4. Gold incremental

```powershell
python -m src.pipeline.gold.build_gold --mode incremental
```

### 5. Gold backfill

```powershell
python -m src.pipeline.gold.build_gold --mode backfill --partitions part_00001.parquet
```

## Publicacion de referencia en GCP

### Requisitos previos

- proyecto activo: `arboreal-logic-493416-i6`
- region operativa correcta en `.env`
- datasets BigQuery existentes: `bronze`, `silver`, `gold`
- bucket GCS existente
- autenticacion valida por ADC o por service account

### Comando

```powershell
python -m src.pipeline.orchestration.publish_gcp_reference
```

### Que hace

- sube `bronze/` a GCS si `GCP_UPLOAD_BRONZE=true`
- sube `silver/` a GCS si `GCP_UPLOAD_SILVER=true`
- carga `gold/card_summary/data.parquet` a `gold.card_summary`
- carga `gold/merchant_summary/data.parquet` a `gold.merchant_summary`
- carga `gold/daily_kpis/data.parquet` a `gold.daily_kpis`

## Validacion

### Tests automatizados

```powershell
pytest
```

### Verificacion en BigQuery

```sql
SELECT * FROM `arboreal-logic-493416-i6.gold.card_summary` LIMIT 10;
SELECT * FROM `arboreal-logic-493416-i6.gold.merchant_summary` LIMIT 10;
SELECT * FROM `arboreal-logic-493416-i6.gold.daily_kpis` LIMIT 10;
```

### Verificacion minima manual

Revisar:

- que existan parquet de bronze, silver y gold
- que el estado incremental se haya actualizado
- que GCS tenga carpetas bajo el prefijo configurado
- que BigQuery tenga tablas pobladas en el dataset `gold`

## Recuperacion basica

Si falla GCP:

1. confirmar el proyecto activo con `gcloud config get-value project`
2. verificar ADC con `gcloud auth application-default print-access-token`
3. confirmar permisos de GCS y BigQuery
4. confirmar bucket y datasets existentes
