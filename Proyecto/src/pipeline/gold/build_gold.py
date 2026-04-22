import argparse
from pathlib import Path
import pandas as pd

from src.pipeline.gold.aggregations import (
    card_summary,
    merchant_summary,
    daily_kpis,
)
from src.pipeline.gold.incremental import (
    get_new_partitions,
    update_state,
    reset_state,
    save_state,
)
from src.pipeline.gold.backfill import resolve_backfill_partitions
from src.pipeline.gold.io_gold import save_parquet, load_existing


BASE_DIR = Path(".")
SILVER_TRANSACTIONS = BASE_DIR / "silver" / "transactions"
GOLD_DIR = BASE_DIR / "gold"


def combine_card_summaries(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    result = (
        df.groupby("card_id", as_index=False)
        .agg(
            total_transactions=("total_transactions", "sum"),
            amount_sum=("amount_sum", "sum"),
            amount_max=("amount_max", "max"),
            unique_merchants=("unique_merchants", "sum"),
            first_txn=("first_txn", "min"),
            last_txn=("last_txn", "max"),
        )
    )

    result["avg_amount"] = result["amount_sum"] / result["total_transactions"]

    result = result[
        [
            "card_id",
            "total_transactions",
            "amount_sum",
            "avg_amount",
            "amount_max",
            "unique_merchants",
            "first_txn",
            "last_txn",
        ]
    ].rename(
        columns={
            "amount_sum": "total_amount",
            "amount_max": "max_amount",
        }
    )

    return result


def combine_merchant_summaries(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    result = (
        df.groupby("merchant_id", as_index=False)
        .agg(
            total_transactions=("total_transactions", "sum"),
            total_sales=("total_sales", "sum"),
            unique_cards=("unique_cards", "sum"),
            first_txn=("first_txn", "min"),
            last_txn=("last_txn", "max"),
        )
    )

    result["avg_ticket"] = result["total_sales"] / result["total_transactions"]

    result = result[
        [
            "merchant_id",
            "total_transactions",
            "total_sales",
            "avg_ticket",
            "unique_cards",
            "first_txn",
            "last_txn",
        ]
    ]

    return result


def combine_daily_kpis(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    result = (
        df.groupby("date", as_index=False)
        .agg(
            total_transactions=("total_transactions", "sum"),
            total_amount=("total_amount", "sum"),
            unique_cards=("unique_cards", "sum"),
            unique_merchants=("unique_merchants", "sum"),
        )
    )

    result["avg_amount"] = result["total_amount"] / result["total_transactions"]
    return result


def build_gold_from_partitions(
    partitions: list[str],
    silver_transactions_path: Path = SILVER_TRANSACTIONS,
    gold_dir: Path = GOLD_DIR,
) -> None:
    card_parts = []
    merchant_parts = []
    daily_parts = []

    for part in partitions:
        part_path = silver_transactions_path / part
        df = pd.read_parquet(part_path)

        if df.empty:
            continue

        if "purchase_date" in df.columns:
            df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")

        card_parts.append(card_summary(df))
        merchant_parts.append(merchant_summary(df))
        daily_parts.append(daily_kpis(df))

    final_card = combine_card_summaries(card_parts)
    final_merchant = combine_merchant_summaries(merchant_parts)
    final_daily = combine_daily_kpis(daily_parts)

    save_parquet(final_card, gold_dir / "card_summary" / "data.parquet")
    save_parquet(final_merchant, gold_dir / "merchant_summary" / "data.parquet")
    save_parquet(final_daily, gold_dir / "daily_kpis" / "data.parquet")

def run_full() -> None:
    partitions = sorted([p.name for p in SILVER_TRANSACTIONS.glob("*.parquet")])
    if not partitions:
        print("No se encontraron particiones en silver/transactions")
        return

    build_gold_from_partitions(partitions)
    save_state(partitions)
    print("Gold full generada correctamente.")


def run_incremental() -> None:
    new_parts, processed = get_new_partitions(SILVER_TRANSACTIONS)

    if not new_parts:
        print("No hay nuevas particiones para procesar.")
        return

    all_parts = processed + new_parts
    build_gold_from_partitions(all_parts)
    update_state(processed, new_parts)

    print(f"Gold incremental actualizada con {len(new_parts)} partición(es).")


def run_backfill(partitions: list[str] | None = None) -> None:
    selected_parts = resolve_backfill_partitions(
        silver_transactions_path=SILVER_TRANSACTIONS,
        partitions=partitions,
    )

    print(f"Backfill solicitado para: {selected_parts}")

    # Opción simple y correcta:
    # reconstruir gold completa desde todas las particiones disponibles
    all_parts = sorted([p.name for p in SILVER_TRANSACTIONS.glob("*.parquet")])

    build_gold_from_partitions(all_parts)
    save_state(all_parts)

    print("Backfill completado. Gold reconstruida completamente.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "backfill"],
        default="full",
    )
    parser.add_argument(
        "--partitions",
        nargs="*",
        help="Lista de particiones para backfill, por ejemplo: part_001.parquet part_002.parquet",
    )

    args = parser.parse_args()

    if args.mode == "full":
        run_full()
    elif args.mode == "incremental":
        run_incremental()
    elif args.mode == "backfill":
        run_backfill(args.partitions)