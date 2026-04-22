import pandas as pd

from src.pipeline.utils_ingestion import normalize_columns, add_ingestion_metadata


def process_transaction_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = normalize_columns(chunk)
    chunk = add_ingestion_metadata(chunk, "historical_transactions_csv")
    return chunk


def ingest_transactions_in_chunks(
    input_path: str,
    logger,
    chunksize: int = 500_000
):
    logger.info(
        f"Leyendo archivo de transacciones por partes: {input_path} | chunksize={chunksize}"
    )

    chunk_iterator = pd.read_csv(input_path, chunksize=chunksize)

    total_rows = 0
    total_chunks = 0

    for chunk in chunk_iterator:
        if chunk.empty:
            continue

        chunk = process_transaction_chunk(chunk)
        total_rows += len(chunk)
        total_chunks += 1

        logger.info(
            f"Chunk {total_chunks} procesado | filas acumuladas: {total_rows}"
        )

        yield chunk

    if total_chunks == 0:
        raise ValueError("historical_transactions.csv está vacío")