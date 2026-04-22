from pathlib import Path

from google.cloud import bigquery


def load_parquet_to_bigquery(
    local_path: Path,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"No existe el archivo local: {local_path}")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=True,
    )

    with open(local_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config,
        )

    job.result()
