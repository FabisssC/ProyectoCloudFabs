from pathlib import Path

from google.cloud import storage


def upload_file_to_gcs(local_path: Path, bucket_name: str, blob_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))


def upload_directory_to_gcs(local_dir: Path, bucket_name: str, prefix: str) -> list[str]:
    if not local_dir.exists():
        raise FileNotFoundError(f"No existe el directorio local: {local_dir}")

    uploaded = []
    for file_path in local_dir.rglob("*"):
        if not file_path.is_file():
            continue

        relative_path = file_path.relative_to(local_dir).as_posix()
        blob_name = f"{prefix.rstrip('/')}/{relative_path}"
        upload_file_to_gcs(file_path, bucket_name, blob_name)
        uploaded.append(blob_name)

    return uploaded
