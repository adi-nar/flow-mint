import polars as pl

def month_over_month(summary: pl.DataFrame) -> pl.DataFrame:
    """
    Adds month-over-month delta columns for influx, total spend, and net savings.
    """
    return summary.with_columns([
        (pl.col("total_influx") - pl.col("total_influx").shift(1)).alias("influx_delta"),
        (pl.col("net_savings")  - pl.col("net_savings").shift(1)).alias("savings_delta"),
    ])

def top_categories(df: pl.DataFrame, n: int = 5) -> pl.DataFrame:
    """Returns top N spending categories by total debit."""
    return (
        df.filter(
            pl.col("debit").is_not_null() &
            (pl.col("category") != "Internal Transfers")
        )
        .group_by("category")
        .agg(pl.col("debit").sum().alias("total_spent"))
        .sort("total_spent", descending=True)
        .head(n)
    )