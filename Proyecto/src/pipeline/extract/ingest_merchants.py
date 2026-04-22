import pandas as pd

from src.pipeline.utils_ingestion import normalize_columns, add_ingestion_metadata


def ingest_merchants(input_path: str, logger) -> pd.DataFrame:
    logger.info(f"Leyendo archivo de merchants: {input_path}")

    df = pd.read_csv(input_path)

    if df.empty:
        raise ValueError("merchants.csv está vacío")

    df = normalize_columns(df)
    df = add_ingestion_metadata(df, "merchants_csv")

    logger.info(f"Merchants cargados: {len(df)} filas, {len(df.columns)} columnas")
    return df