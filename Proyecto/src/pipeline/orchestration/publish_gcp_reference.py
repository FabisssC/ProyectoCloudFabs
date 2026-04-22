import os
from pathlib import Path

from dotenv import load_dotenv

from src.pipeline.load.bigquery_factory_glue import load_parquet_to_bigquery
from src.pipeline.load.gcs_s3_blob import upload_directory_to_gcs


BASE_DIR = Path(".")
BRONZE_DIR = BASE_DIR / "bronze"
SILVER_DIR = BASE_DIR / "silver"
GOLD_DIR = BASE_DIR / "gold"


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def publish_reference_to_gcp() -> None:
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = os.getenv("GCS_BUCKET")
    gcs_prefix = os.getenv("GCS_PREFIX", "fraud_medallion")
    dataset_gold = os.getenv("BQ_DATASET_GOLD", "gold")
    write_disposition = os.getenv("BQ_WRITE_DISPOSITION", "WRITE_TRUNCATE")

    upload_bronze = _as_bool(os.getenv("GCP_UPLOAD_BRONZE", "true"), default=True)
    upload_silver = _as_bool(os.getenv("GCP_UPLOAD_SILVER", "true"), default=True)
    load_gold = _as_bool(os.getenv("GCP_LOAD_GOLD_TO_BIGQUERY", "true"), default=True)

    if not project_id:
        raise ValueError("Falta GCP_PROJECT_ID en el entorno")

    if (upload_bronze or upload_silver) and not bucket_name:
        raise ValueError("Falta GCS_BUCKET en el entorno")

    if upload_bronze:
        uploaded = upload_directory_to_gcs(BRONZE_DIR, bucket_name, f"{gcs_prefix}/bronze")
        print(f"Archivos bronze subidos a GCS: {len(uploaded)}")

    if upload_silver:
        uploaded = upload_directory_to_gcs(SILVER_DIR, bucket_name, f"{gcs_prefix}/silver")
        print(f"Archivos silver subidos a GCS: {len(uploaded)}")

    if load_gold:
        gold_tables = {
            "card_summary": GOLD_DIR / "card_summary" / "data.parquet",
            "merchant_summary": GOLD_DIR / "merchant_summary" / "data.parquet",
            "daily_kpis": GOLD_DIR / "daily_kpis" / "data.parquet",
        }

        for table_name, local_path in gold_tables.items():
            load_parquet_to_bigquery(
                local_path=local_path,
                project_id=project_id,
                dataset_id=dataset_gold,
                table_id=table_name,
                write_disposition=write_disposition,
            )
            print(f"Tabla cargada en BigQuery: {dataset_gold}.{table_name}")


if __name__ == "__main__":
    publish_reference_to_gcp()
