from pathlib import Path
import pandas as pd

from .validate import load_schema, validate_dataframe


BASE_DIR = Path(__file__).resolve().parents[3]

BRONZE_DIR = BASE_DIR / "bronze"
BRONZE_TRANSACTIONS_DIR = BRONZE_DIR / "transactions"

SILVER_DIR = BASE_DIR / "silver"
SILVER_TRANSACTIONS_DIR = SILVER_DIR / "transactions"

SCHEMA_DIR = BASE_DIR / "data_contracts" / "schema"


def load_reference_data():
    merchants = pd.read_parquet(BRONZE_DIR / "bronze_merchants.parquet")
    fx = pd.read_parquet(BRONZE_DIR / "bronze_fx_rates.parquet")
    return merchants, fx


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    df["card_id"] = df["card_id"].astype("string")
    df["merchant_id"] = df["merchant_id"].astype("string")
    df["authorized_flag"] = df["authorized_flag"].astype("string")
    df["category_1"] = df["category_1"].astype("string")
    df["category_3"] = df["category_3"].astype("string")
    df["source_name"] = df["source_name"].astype("string")
    df["ingest_ts"] = df["ingest_ts"].astype("string")

    df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")
    df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")

    numeric_cols = [
        "city_id",
        "installments",
        "merchant_category_id",
        "month_lag",
        "category_2",
        "state_id",
        "subsector_id",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["card_id", "purchase_amount", "purchase_date", "source_name", "ingest_ts"])

    return df


def clean_merchants(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    string_cols = [
        "merchant_id",
        "category_1",
        "most_recent_sales_range",
        "most_recent_purchases_range",
        "category_4",
        "source_name",
        "ingest_ts",
    ]

    for col in string_cols:
        df[col] = df[col].astype("string")

    numeric_cols = [
        "merchant_group_id",
        "merchant_category_id",
        "subsector_id",
        "numerical_1",
        "numerical_2",
        "avg_sales_lag3",
        "avg_purchases_lag3",
        "active_months_lag3",
        "avg_sales_lag6",
        "avg_purchases_lag6",
        "active_months_lag6",
        "avg_sales_lag12",
        "avg_purchases_lag12",
        "active_months_lag12",
        "city_id",
        "state_id",
        "category_2",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["merchant_id", "source_name", "ingest_ts"])

    return df


def clean_fx(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    df["base_currency"] = df["base_currency"].astype("string").str.upper().str.strip()
    df["target_currency"] = df["target_currency"].astype("string").str.upper().str.strip()
    df["source_name"] = df["source_name"].astype("string")
    df["ingest_ts"] = df["ingest_ts"].astype("string")

    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df["rate_date"] = pd.to_datetime(df["rate_date"], errors="coerce")

    df = df.dropna(subset=["base_currency", "target_currency", "rate", "rate_date", "source_name", "ingest_ts"])

    return df


def enrich_transactions(transactions: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
    df = transactions.merge(
        merchants,
        on="merchant_id",
        how="left",
        suffixes=("_txn", "_mrch")
    )

    return df


def process_transaction_parts():
    bronze_transactions_schema = load_schema(SCHEMA_DIR / "bronze_transactions.json")
    bronze_merchants_schema = load_schema(SCHEMA_DIR / "bronze_merchants.json")
    bronze_fx_schema = load_schema(SCHEMA_DIR / "bronze_fx_rates.json")
    silver_schema = load_schema(SCHEMA_DIR / "silver_enriched_transactions.json")

    merchants, fx = load_reference_data()

    validate_dataframe(merchants, bronze_merchants_schema)
    validate_dataframe(fx, bronze_fx_schema)

    merchants = clean_merchants(merchants)
    fx = clean_fx(fx)

    SILVER_TRANSACTIONS_DIR.mkdir(parents=True, exist_ok=True)

    transaction_parts = sorted(BRONZE_TRANSACTIONS_DIR.glob("part_*.parquet"))

    if not transaction_parts:
        raise FileNotFoundError(f"No se encontraron archivos part_*.parquet en {BRONZE_TRANSACTIONS_DIR}")

    for part_path in transaction_parts:
        print(f"Procesando {part_path.name}...")

        transactions = pd.read_parquet(part_path)

        validate_dataframe(transactions, bronze_transactions_schema)

        transactions = clean_transactions(transactions)

        silver_part = enrich_transactions(transactions, merchants)

        validate_dataframe(silver_part, silver_schema)

        output_path = SILVER_TRANSACTIONS_DIR / part_path.name
        silver_part.to_parquet(output_path, index=False)

        print(f"Generado: {output_path.name}")

    fx_output_path = SILVER_DIR / "silver_fx_rates.parquet"
    fx.to_parquet(fx_output_path, index=False)
    print(f"Generado: {fx_output_path.name}")


def run():
    process_transaction_parts()
    print("Silver por partes generado correctamente")


if __name__ == "__main__":
    run()