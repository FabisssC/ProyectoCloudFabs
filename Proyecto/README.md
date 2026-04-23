# Pipeline Multicloud de Fraude Financiero

## Resumen

Este proyecto implementa un pipeline medallion para crear una capa gold que ayude al analisis de fraude financiero a partir de:

- transacciones historicas en CSV
- catalogo de merchants en CSV
- tipo de cambio diario desde API

La referencia cloud principal es GCP. A la fecha ya estan implementadas las capas `bronze`, `silver` y `gold`, junto con validacion por contratos, procesamiento por particiones, incremental simulado y backfill controlado.

## Estado por etapas

- Etapa 1: estructura base, README, arquitectura y entorno
- Etapa 2: ingesta local a `bronze`
- Etapa 3: contratos de datos y validacion automatica
- Etapa 4: limpieza e integracion para `silver`
- Etapa 5: capa `gold`, incremental y backfill
- Etapa 6: configuracion por `.env`, `Makefile`, `RUNBOOK`, CI y referencia de publicacion a GCP

## Arquitectura funcional

### Bronze

Zona cruda con normalizacion ligera y metadatos de ingesta.

Salidas:

- `bronze/transactions/part_*.parquet`
- `bronze/bronze_merchants.parquet`
- `bronze/bronze_fx_rates.parquet`

### Silver

Zona limpia e integrada.

Salidas:

- `silver/transactions/part_*.parquet`
- `silver/silver_fx_rates.parquet`

### Gold

Zona analitica construida a partir de `silver/transactions`.

Salidas:

- `gold/card_summary/data.parquet`
- `gold/merchant_summary/data.parquet`
- `gold/daily_kpis/data.parquet`

## Etapa 5: Gold, incremental y backfill

La capa gold se implemento con una estructura modular:

- `src/pipeline/gold/aggregations.py`
- `src/pipeline/gold/build_gold.py`
- `src/pipeline/gold/incremental.py`
- `src/pipeline/gold/backfill.py`
- `src/pipeline/gold/io_gold.py`

### Salidas analiticas

- `card_summary`
- `merchant_summary`
- `daily_kpis`

### Caracteristicas tecnicas

- procesamiento por particiones de `silver/transactions`
- sin cargar toda la capa silver en memoria
- incremental simulado por deteccion de particiones nuevas
- archivo de estado en `gold/_state/processed_partitions.json`
- backfill controlado para reproceso consistente

## Contratos y calidad

Los contratos activos viven en `data_contracts/schema/`:

- `bronze_transactions.json`
- `bronze_merchants.json`
- `bronze_fx_rates.json`
- `silver_enriched_transactions.json`

La validacion automatica verifica:

- columnas obligatorias
- tipos esperados
- nulabilidad

## Testing

El proyecto ya tiene tests para:

- contratos de datos
- transformaciones de silver
- agregaciones de gold
- incremental con nuevas particiones

Resumen actual reportado por tu avance:

- 8 tests aprobados

## Ejecucion local

### Preparar entorno

```powershell
cd Proyecto
python -m venv_proy .venv
.venv_proy\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt
Copy-Item .env.example .env
```

### Bronze

```powershell
python -m src.pipeline.orchestration.run_local_ingestion
```

### Silver

```powershell
python -m src.pipeline.transform.build_silver
```

### Gold full

```powershell
python -m src.pipeline.gold.build_gold --mode full
```

### Gold incremental

```powershell
python -m src.pipeline.gold.build_gold --mode incremental
```

### Gold backfill

```powershell
python -m src.pipeline.gold.build_gold --mode backfill --partitions part_00001.parquet
```

## Etapa 6: Referencia GCP

Configuracion actual del proyecto:

- nombre del proyecto: `fraud-medallion-pipeline`
- project id: `arboreal-logic-493416-i6`
- region operativa: `us-east1`
- cuenta de servicio planeada: `sa-medallion-pipeline`
- datasets BigQuery creados: `bronze`, `silver`, `gold`

La implementacion de referencia preparada en esta etapa hace lo siguiente:

- publica `bronze/` y `silver/` a GCS
- publica `gold` en BigQuery
- deja comandos de automatizacion en `Makefile`
- deja CI minima en GitHub Actions

### Autenticacion usada en desarrollo

Durante la validacion local de la referencia GCP se utilizo autenticacion mediante Application Default Credentials con `gcloud auth application-default login`, lo que permitio ejecutar pruebas reales sin almacenar archivos sensibles dentro del repositorio.

Esta estrategia fue adecuada para desarrollo. Para una version operativa o productiva, se recomienda migrar a una cuenta de servicio dedicada con permisos minimos sobre GCS y BigQuery, evitando dependencia de credenciales interactivas de usuario.

### Publicacion GCP

```powershell
python -m src.pipeline.orchestration.publish_gcp_reference
```

## Validacion en BigQuery

Ejemplos de consultas para validar las tablas publicadas:

```sql
SELECT * FROM `arboreal-logic-493416-i6.gold.card_summary` LIMIT 10;
SELECT * FROM `arboreal-logic-493416-i6.gold.merchant_summary` LIMIT 10;
SELECT * FROM `arboreal-logic-493416-i6.gold.daily_kpis` LIMIT 10;
```

## Deteccion de fraude habilitada por gold

La validacion de fraude no se abordo mediante un modelo predictivo, sino a traves de la generacion de agregaciones analiticas en la capa gold. Estas permiten identificar comportamientos atipicos mediante reglas de negocio simples, tales como volumenes inusuales de transacciones, montos elevados o patrones anomalos por tarjeta, comercio o dia.

De esta manera, el pipeline habilita la deteccion de posibles casos de fraude a partir de analisis exploratorio y reglas definidas sobre las tablas finales en BigQuery.

Ejemplo de clasificacion simple de riesgo sobre `card_summary`:

```sql
SELECT *,
  CASE
    WHEN total_amount > 100000 THEN 'HIGH_RISK'
    WHEN total_transactions > 50 THEN 'MEDIUM_RISK'
    ELSE 'LOW_RISK'
  END AS risk_level
FROM `arboreal-logic-493416-i6.gold.card_summary`;
```

## Estructura del repositorio

```text
Proyecto/
|-- .github/workflows/ci.yml
|-- architecture/
|-- bronze/
|-- data_contracts/schema/
|-- docs/RUNBOOK.md
|-- gold/
|   |-- _state/
|   |-- card_summary/
|   |-- merchant_summary/
|   `-- daily_kpis/
|-- silver/
|-- src/pipeline/
|   |-- extract/
|   |-- gold/
|   |-- load/
|   |-- orchestration/
|   `-- transform/
|-- tests/
|-- .env.example
|-- Makefile
|-- pyproject.toml
|-- requirements.txt
`-- requirements-dev.txt
```

## Siguientes pasos

1. mantener ADC para desarrollo local
2. si se quiere operacion no interactiva, crear credenciales de service account fuera del repositorio
3. ampliar CI con lint y cobertura si la estructura ya no cambiara
