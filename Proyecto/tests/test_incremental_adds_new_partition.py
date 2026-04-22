def test_incremental_adds_new_partition(tmp_path):
    import pandas as pd
    from src.pipeline.gold.build_gold import build_gold_from_partitions

    silver_path = tmp_path / "silver" / "transactions"
    gold_path = tmp_path / "gold"

    silver_path.mkdir(parents=True)
    gold_path.mkdir(parents=True)

    df1 = pd.DataFrame({
        "card_id": [1],
        "purchase_amount": [10],
        "merchant_id": [100],
        "purchase_date": ["2020-01-01"]
    })
    df1.to_parquet(silver_path / "part1.parquet")

    df2 = pd.DataFrame({
        "card_id": [1],
        "purchase_amount": [20],
        "merchant_id": [101],
        "purchase_date": ["2020-01-02"]
    })
    df2.to_parquet(silver_path / "part2.parquet")

    build_gold_from_partitions(
        ["part1.parquet", "part2.parquet"],
        silver_transactions_path=silver_path,
        gold_dir=gold_path,
    )

    result = pd.read_parquet(gold_path / "card_summary" / "data.parquet")

    assert len(result) == 1
    assert result.iloc[0]["card_id"] == 1