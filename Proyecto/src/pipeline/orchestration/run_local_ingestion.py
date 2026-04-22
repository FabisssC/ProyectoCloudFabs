import os

from dotenv import load_dotenv

from src.pipeline.extract.ingest_transactions import ingest_transactions_in_chunks
from src.pipeline.extract.ingest_merchants import ingest_merchants
from src.pipeline.extract.ingest_fx import (
    DEFAULT_FX_API_URL,
    DEFAULT_FX_TIMEOUT_SEC,
    ingest_fx,
)
from src.pipeline.utils_ingestion import (
    setup_logger,
    save_dataframe,
    save_parquet_parts,
)


def main() -> None:
    load_dotenv()
    logger = setup_logger()

    transactions_path = os.getenv(
        "INPUT_TRANSACTIONS_PATH",
        "data_sources/elo/historical_transactions.csv"
    )
    merchants_path = os.getenv(
        "INPUT_MERCHANTS_PATH",
        "data_sources/elo/merchants.csv"
    )
    fx_api_url = os.getenv(
        "FX_API_URL",
        DEFAULT_FX_API_URL,
    )
    api_timeout_sec = int(os.getenv("API_TIMEOUT_SEC", str(DEFAULT_FX_TIMEOUT_SEC)))
    bronze_dir = os.getenv("BRONZE_DIR", "bronze")
    transactions_chunksize = int(os.getenv("TRANSACTIONS_CHUNKSIZE", "500000"))

    logger.info("Iniciando pipeline local de ingesta a bronze")

    try:
        output_transactions_dir = os.path.join(bronze_dir, "transactions")

        df_iterator = ingest_transactions_in_chunks(
            transactions_path,
            logger,
            chunksize=transactions_chunksize,
        )

        save_parquet_parts(
            df_iterator,
            output_transactions_dir,
            prefix="part"
        )

        logger.info(f"Archivos generados en: {output_transactions_dir}")
    except Exception as exc:
        logger.error(f"Fallo ingest_transactions: {exc}")

    try:
        output_merchants = os.path.join(
            bronze_dir, "bronze_merchants.parquet"
        )
        df_merchants = ingest_merchants(merchants_path, logger)
        save_dataframe(df_merchants, output_merchants)
        logger.info(f"Archivo generado: {output_merchants}")
    except Exception as exc:
        logger.error(f"Fallo ingest_merchants: {exc}")

    try:
        output_fx = os.path.join(
            bronze_dir, "bronze_fx_rates.parquet"
        )
        df_fx = ingest_fx(fx_api_url, api_timeout_sec, logger)
        save_dataframe(df_fx, output_fx)
        logger.info(f"Archivo generado: {output_fx}")
    except Exception as exc:
        logger.error(f"Fallo ingest_fx: {exc}")

    logger.info("Pipeline local de ingesta finalizado")


if __name__ == "__main__":
    main()
