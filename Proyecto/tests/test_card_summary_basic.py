def test_card_summary_basic():
    import pandas as pd
    from src.pipeline.gold.aggregations import card_summary

    df = pd.DataFrame({
        "card_id": [1, 1, 1],
        "purchase_amount": [10, 20, 30],
        "merchant_id": [100, 101, 102],
        "purchase_date": ["2020-01-01", "2020-01-02", "2020-01-03"]
    })

    res = card_summary(df)
    row = res.iloc[0]

    assert row["total_transactions"] == 3
    assert row["amount_sum"] == 60
    assert round(row["avg_amount"], 2) == 20.00
    assert row["amount_max"] == 30