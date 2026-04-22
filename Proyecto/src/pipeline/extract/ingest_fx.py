import logging
from pathlib import Path

import pandas as pd
import requests

from src.pipeline.transform.validate import load_schema, validate_dataframe
from src.pipeline.utils_ingestion import add_ingestion_metadata


BASE_DIR = Path(__file__).resolve().parents[3]
BRONZE_DIR = BASE_DIR / "bronze"
SCHEMA_PATH = BASE_DIR / "data_contracts" / "schema" / "bronze_fx_rates.json"

DEFAULT_FX_API_URL = "https://open.er-api.com/v6/latest/USD"
DEFAULT_FX_TIMEOUT_SEC = 20


def ingest_fx(api_url: str, timeout_sec: int, logger) -> pd.DataFrame:
    logger.info(f"Consultando API FX: {api_url}")

    try:
        response = requests.get(api_url, timeout=timeout_sec)
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise TimeoutError("La API FX excedió el tiempo de espera") from exc
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Error al consultar la API FX: {exc}") from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise ValueError("La API FX no devolvió un JSON válido") from exc

    if not data:
        raise ValueError("La API FX devolvió una respuesta vacía")

    if "rates" not in data:
        raise KeyError("La API FX no devolvió la clave 'rates'")

    rates = data["rates"]

    if not isinstance(rates, dict) or len(rates) == 0:
        raise ValueError("La clave 'rates' está vacía o no tiene formato válido")

    base_currency = data.get("base_code") or data.get("base") or "USD"
    rate_date = data.get("time_last_update_utc") or data.get("date")

    records = []
    for target_currency, rate in rates.items():
        records.append(
            {
                "base_currency": base_currency,
                "target_currency": target_currency,
                "rate": rate,
                "rate_date": rate_date,
            }
        )

    df = pd.DataFrame(records)

    if df.empty:
        raise ValueError("No se pudieron construir registros a partir de la API FX")

    df = add_ingestion_metadata(df, "fx_api")

    logger.info(f"Tipos de cambio cargados: {len(df)} filas")
    return df


def run():
    logger = logging.getLogger("ingest_fx")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    df = ingest_fx(
        api_url=DEFAULT_FX_API_URL,
        timeout_sec=DEFAULT_FX_TIMEOUT_SEC,
        logger=logger
    )

    schema = load_schema(SCHEMA_PATH)
    validate_dataframe(df, schema)

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = BRONZE_DIR / "bronze_fx_rates.parquet"
    df.to_parquet(output_path, index=False)

    logger.info(f"Archivo generado: {output_path}")


if __name__ == "__main__":
    run()
