# GCP Reference

## Proyecto actual

- nombre: `fraud-medallion-pipeline`
- project id: `arboreal-logic-493416-i6`
- region operativa: `us-east1`
- cuenta de servicio prevista: `sa-medallion-pipeline`

## Datasets existentes

- `bronze`
- `silver`
- `gold`

## Flujo aplicado en desarrollo

Para validacion local se utilizo autenticacion con Application Default Credentials:

```powershell
gcloud auth login
gcloud config set project arboreal-logic-493416-i6
gcloud auth application-default login
gcloud auth application-default set-quota-project arboreal-logic-493416-i6
```

Esto permitio ejecutar pruebas reales sin guardar un archivo JSON sensible dentro del repositorio.

## Alternativa operativa con service account

Si se quiere una autenticacion no interactiva:

1. crear el proyecto o usar uno dedicado en Google Cloud
2. crear el bucket GCS del pipeline
3. crear una cuenta de servicio dedicada
4. otorgar permisos minimos de GCS y BigQuery
5. generar y descargar una clave JSON
6. guardar el archivo fuera del repositorio
7. exportar `GOOGLE_APPLICATION_CREDENTIALS` apuntando a esa ruta

## Uso en este repositorio

La referencia GCP actual esta orientada a:

- subir `bronze/` y `silver/` a GCS
- cargar `gold/card_summary`, `gold/merchant_summary` y `gold/daily_kpis` en BigQuery

## Variables necesarias

Configurar en `.env`:

- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCS_BUCKET`
- `BQ_DATASET_GOLD`

`GOOGLE_APPLICATION_CREDENTIALS` es opcional si trabajas con ADC.

## Comando de publicacion

```powershell
python -m src.pipeline.orchestration.publish_gcp_reference
```
