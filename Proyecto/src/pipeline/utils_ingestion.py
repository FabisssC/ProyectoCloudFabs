import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("local_ingestion")

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def normalize_column_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", "_", name)
    return name


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_column_name(col) for col in df.columns]
    return df


def add_ingestion_metadata(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    df = df.copy()
    df["source_name"] = source_name
    df["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    return df


def ensure_parent_dir(output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(dir_path: str) -> None:
    Path(dir_path).mkdir(parents=True, exist_ok=True)


def save_dataframe(df: pd.DataFrame, output_path: str) -> None:
    ensure_parent_dir(output_path)

    if output_path.endswith(".parquet"):
        df.to_parquet(output_path, index=False)
    elif output_path.endswith(".csv"):
        df.to_csv(output_path, index=False)
    else:
        raise ValueError("Formato de salida no soportado. Usa .parquet o .csv")


def save_parquet_parts(df_iterator, output_dir: str, prefix: str = "part") -> None:
    ensure_dir(output_dir)

    for idx, df in enumerate(df_iterator, start=1):
        output_path = Path(output_dir) / f"{prefix}_{idx:05d}.parquet"
        df.to_parquet(output_path, index=False)