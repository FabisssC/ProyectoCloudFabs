import json
import pandas as pd
from pandas.api.types import (
    is_string_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_datetime64_any_dtype,
)


def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _matches_type(series: pd.Series, expected_type: str) -> bool:
    if expected_type == "string":
        return is_string_dtype(series) or series.dtype == object
    if expected_type == "int":
        return is_integer_dtype(series)
    if expected_type == "float":
        return is_float_dtype(series) or is_integer_dtype(series)
    if expected_type == "datetime":
        return is_datetime64_any_dtype(series)
    return False


def validate_dataframe(df: pd.DataFrame, schema: dict):
    expected_cols = schema["columns"]

    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias del contrato: {missing}")

    for col, props in expected_cols.items():
        expected_type = props["type"]

        if not _matches_type(df[col], expected_type):
            raise TypeError(
                f"Tipo incorrecto en columna '{col}'. "
                f"Esperado: {expected_type}. Actual: {df[col].dtype}"
            )

        if not props["nullable"] and df[col].isna().any():
            raise ValueError(f"La columna '{col}' contiene nulos no permitidos")

    return True