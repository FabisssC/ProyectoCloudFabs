import pandas as pd
from src.pipeline.transform.build_silver import clean_transactions, enrich_transactions


def test_clean_transactions_converts_date():
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

    cleaned = clean_transactions(df)

    assert pd.api.types.is_datetime64_any_dtype(cleaned["purchase_date"])


def test_enrich_transactions_adds_merchant_data():
    transactions = pd.DataFrame({
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
        "purchase_date": pd.to_datetime(["2020-01-01"]),
        "category_2": [1.0],
        "state_id": [5],
        "subsector_id": [2],
        "source_name": ["test"],
        "ingest_ts": ["2026-01-01"]
    })

    merchants = pd.DataFrame({
        "merchant_id": ["M1"],
        "merchant_group_id": [99],
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

    enriched = enrich_transactions(transactions, merchants)

    assert "merchant_group_id" in enriched.columns
    assert enriched["merchant_group_id"].iloc[0] == 99


def test_enrich_transactions_handles_missing_merchant():
    transactions = pd.DataFrame({
        "authorized_flag": ["Y"],
        "card_id": ["C1"],
        "city_id": [1],
        "category_1": ["A"],
        "installments": [1],
        "category_3": ["B"],
        "merchant_category_id": [10],
        "merchant_id": ["M_UNKNOWN"],
        "month_lag": [0],
        "purchase_amount": [100.0],
        "purchase_date": pd.to_datetime(["2020-01-01"]),
        "category_2": [1.0],
        "state_id": [5],
        "subsector_id": [2],
        "source_name": ["test"],
        "ingest_ts": ["2026-01-01"]
    })

    merchants = pd.DataFrame({
        "merchant_id": ["M1"],
        "merchant_group_id": [99]
    })

    enriched = enrich_transactions(transactions, merchants)

    assert enriched.shape[0] == 1
    assert enriched["merchant_group_id"].isna().iloc[0]