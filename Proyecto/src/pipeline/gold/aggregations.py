import pandas as pd


def card_summary(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby("card_id", as_index=False)
        .agg(
            txn_count=("card_id", "count"),
            amount_sum=("purchase_amount", "sum"),
            amount_max=("purchase_amount", "max"),
            unique_merchants=("merchant_id", "nunique"),
            first_txn=("purchase_date", "min"),
            last_txn=("purchase_date", "max"),
        )
    )

    result["avg_amount"] = result["amount_sum"] / result["txn_count"]
    result = result.rename(columns={"txn_count": "total_transactions"})
    return result


def merchant_summary(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby("merchant_id", as_index=False)
        .agg(
            txn_count=("merchant_id", "count"),
            sales_sum=("purchase_amount", "sum"),
            unique_cards=("card_id", "nunique"),
            first_txn=("purchase_date", "min"),
            last_txn=("purchase_date", "max"),
        )
    )

    result["avg_ticket"] = result["sales_sum"] / result["txn_count"]
    result = result.rename(
        columns={
            "txn_count": "total_transactions",
            "sales_sum": "total_sales",
        }
    )
    return result


def daily_kpis(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp["date"] = pd.to_datetime(tmp["purchase_date"], errors="coerce").dt.date

    result = (
        tmp.groupby("date", as_index=False)
        .agg(
            total_transactions=("date", "count"),
            total_amount=("purchase_amount", "sum"),
            unique_cards=("card_id", "nunique"),
            unique_merchants=("merchant_id", "nunique"),
        )
    )

    result["avg_amount"] = result["total_amount"] / result["total_transactions"]
    return result