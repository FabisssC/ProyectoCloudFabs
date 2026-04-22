import pytest
import pandas as pd
from src.pipeline.transform.validate import validate_dataframe, load_schema


def test_transactions_schema_ok():
    df = pd.DataFrame({
        "authorized_flag": ["Y"],
        "card_id": ["C1"],
        "city_id": [1],
        "category_1": ["A"],
        "installments": [1],
        "category_3": ["B"],
        "merchant_category_id": [10],
        "merchant_id": ["M1"],
        "month_lag": [0],
        "purchase_amount": [100.0],
        "purchase_date": ["2020-01-01"],
        "category_2": [1.0],
        "state_id": [5],
        "subsector_id": [2],
        "source_name": ["test"],
        "ingest_ts": ["2026-01-01"]
    })

    schema = load_schema("data_contracts/schema/bronze_transactions.json")

    assert validate_dataframe(df, schema)


def test_transactions_missing_column():
    df = pd.DataFrame({
        "card_id": ["C1"],
        "purchase_amount": [100.0],
        "purchase_date": ["2020-01-01"],
        "source_name": ["test"],
        "ingest_ts": ["2026-01-01"]
    })

    schema = load_schema("data_contracts/schema/bronze_transactions.json")

    with pytest.raises(ValueError):
        validate_dataframe(df, schema)


def test_merchants_schema_ok():
    df = pd.DataFrame({
        "merchant_id": ["M1"],
        "merchant_group_id": [1],
        "merchant_category_id": [10],
        "subsector_id": [2],
        "numerical_1": [0.1],
        "numerical_2": [0.2],
        "category_1": ["A"],
        "most_recent_sales_range": ["E"],
        "most_recent_purchases_range": ["E"],
        "avg_sales_lag3": [1.0],
        "avg_purchases_lag3": [1.0],
        "active_months_lag3": [1],
        "avg_sales_lag6": [1.0],
        "avg_purchases_lag6": [1.0],
        "active_months_lag6": [1],
        "avg_sales_lag12": [1.0],
        "avg_purchases_lag12": [1.0],
        "active_months_lag12": [1],
        "category_4": ["A"],
        "city_id": [1],
        "state_id": [1],
        "category_2": [1.0],
        "source_name": ["test"],
        "ingest_ts": ["2026-01-01"]
    })

    schema = load_schema("data_contracts/schema/bronze_merchants.json")

    assert validate_dataframe(df, schema)