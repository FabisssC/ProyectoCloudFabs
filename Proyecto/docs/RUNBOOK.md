# RUNBOOK

## Objetivo

Dejar instrucciones minimas para preparar el entorno local del pipeline y evitar configuraciones no replicables.

## Preparacion local

### 1. Crear entorno virtual

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

### 2. Instalar dependencias

Runtime:

```bash
pip install -r requirements.txt
```

Runtime y desarrollo:

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

### 3. Configurar variables de entorno

```powershell
Copy-Item .env.example .env
```

Ajustar al menos:

- `PROJECT_ENV`
- `DATA_DIR`
- `OUTPUT_DIR`
- `CLOUD_PROVIDER`
- `GCP_PROJECT_ID`
- `GCS_BUCKET`
- `BQ_DATASET_BRONZE`
- `BQ_DATASET_SILVER`
- `BQ_DATASET_GOLD`

## Estructura esperada de datos locales

- `data_sources/elo/` para CSV de transacciones y merchants
- `data_sources/fx/` para snapshots o cache de API
- `out/` para salidas locales temporales

## Ejecucion esperada por etapa

### Dia 1

- validar entorno
- confirmar variables de entorno
- validar estructura del repositorio

### Dia 2 en adelante

Se agregaran comandos concretos para:

- ingest a bronze
- transform a silver
- publicacion a gold
- backfill

## Recuperacion basica

Si el entorno falla:

1. eliminar `.venv`
2. recrear el entorno
3. reinstalar dependencias
4. validar que `.env` exista y tenga rutas correctas

## Notas

- El entorno virtual recomendado vive dentro de `Proyecto/.venv`
- No se debe depender de `venv_proy/` fuera de la carpeta del proyecto
