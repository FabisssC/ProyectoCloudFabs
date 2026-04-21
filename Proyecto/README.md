# Pipeline Multicloud de Fraude Financiero

## Resumen

Este proyecto construye un pipeline de datos con arquitectura medallion para integrar tres fuentes heterogeneas:

- Transacciones de tarjetas
- Catalogo de merchants
- Tipo de cambio diario desde API

El objetivo es dejar una base reproducible para ingerir datos en `bronze`, limpiarlos y enriquecerlos en `silver`, y publicar metricas analiticas en `gold` para analisis de fraude.

La nube principal del proyecto sera GCP, con una extension secundaria pensada para AWS en la etapa final.

## Alcance del Dia 1

El Dia 1 deja cerrados estos puntos:

- Estructura base del repositorio
- Archivos obligatorios iniciales
- Modelo de datos de las tres fuentes
- Diseno funcional bronze / silver / gold
- Clave de enriquecimiento principal
- Estrategia incremental inicial
- Instrucciones de entorno local y dependencias

## Estructura del repositorio

```text
Proyecto/
|-- architecture/
|   |-- architecture.md
|   |-- adr/
|   `-- diagrams/
|-- data_contracts/
|   |-- expectations/
|   `-- schema/
|-- docs/
|   `-- RUNBOOK.md
|-- documentation/
|   |-- COST_NOTES.md
|   |-- RUNBOOK.md
|   `-- SECURITY_NOTES.md
|-- infra/
|   |-- aws/
|   |-- azure/
|   `-- gcp/
|-- notebooks/
|-- src/
|   `-- pipeline/
|-- tests/
|-- .env.example
|-- .gitignore
|-- Makefile
|-- pyproject.toml
|-- requirements.txt
|-- requirements-dev.txt
|-- requirements-aws.txt
`-- requirements-azure.txt
```

`Proyecto/` es la carpeta de trabajo del proyecto. Aunque el repositorio Git vive un nivel arriba, la configuracion reproducible ya queda definida aqui para que no dependas de archivos sueltos fuera de esta carpeta.

## Fuentes y modelo de datos

### 1. Transacciones

Fuente principal en CSV.

Campos esperados a nivel funcional:

- `transaction_id`
- `card_id`
- `merchant_id`
- `purchase_timestamp`
- `purchase_amount`
- `currency_code`
- `installments`
- `authorized_flag`
- `fraud_flag`

### 2. Merchants

Catalogo maestro en CSV.

Campos esperados:

- `merchant_id`
- `merchant_name`
- `merchant_category`
- `country_code`
- `city`
- `state`

### 3. Tipo de cambio

Fuente externa por API.

Campos esperados:

- `rate_date`
- `base_currency`
- `target_currency`
- `exchange_rate`
- `provider`
- `ingestion_timestamp`

## Diseno bronze / silver / gold

### Bronze

Datos crudos con normalizacion minima y trazabilidad de origen.

- `bronze_transactions`
- `bronze_merchants`
- `bronze_fx_rates`

### Silver

Datos limpios, tipados y enriquecidos.

- `silver_enriched_transactions`

Reglas iniciales:

- normalizar nombres de columnas
- convertir fechas y montos
- eliminar duplicados tecnicos
- enriquecer con `merchant_id`
- convertir moneda usando tipo de cambio por fecha

### Gold

Metrica analitica para consumo final.

- `gold_fraud_by_category`
- `gold_fraud_by_country`
- `gold_fraud_adjusted_amount`

## Decisiones clave del pipeline

### Clave de enriquecimiento

La clave principal de join sera `merchant_id`.

Motivos:

- es la relacion mas estable entre transacciones y catalogo de merchants
- evita joins ambiguos por nombre de comercio
- facilita validaciones posteriores en silver

### Logica incremental

La estrategia inicial sera incremental por watermark local usando `purchase_timestamp`.

Reglas:

- guardar el ultimo timestamp procesado en un archivo de estado
- procesar solo registros con fecha posterior al watermark
- permitir backfill manual para reprocesar todo o desde una fecha dada

## Entorno local

### Version recomendada

- Python 3.10 o superior

### Crear entorno virtual

PowerShell:

```powershell
cd Proyecto
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Git Bash o Linux/macOS:

```bash
cd Proyecto
python -m venv .venv
source .venv/bin/activate
```

### Instalar dependencias

Solo runtime:

```bash
pip install -r requirements.txt
```

Runtime + herramientas de desarrollo:

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

### Variables de entorno

1. Copiar `.env.example` a `.env`
2. Ajustar rutas locales, proyecto GCP y buckets segun tu entorno

## Estado actual

El repositorio ya queda listo como base formal del Dia 1. La implementacion de ingesta local del Dia 2 todavia esta pendiente.

## Siguientes pasos

1. Implementar lectura de transacciones CSV
2. Implementar lectura de merchants CSV
3. Implementar extraccion de FX desde API
4. Persistir salidas en `bronze`
