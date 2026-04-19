import polars as pl
from pathlib import Path

class ParquetStore:
    """
    Persists parsed + categorized transactions as Parquet.
    One file per bank per month: data/<bank>/<YYYY-MM>.parquet
    Master file: data/all_transactions.parquet
    """
    def __init__(self, base_dir: str = "data"):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.master = self.base / "all_transactions.parquet"

    def save(self, df: pl.DataFrame, bank: str, month: str):
        """Save transactions for a bank+month slice."""
        dest = self.base / bank
        dest.mkdir(exist_ok=True)
        df.write_parquet(dest / f"{month}.parquet")
        self._update_master(df)

    def _update_master(self, new_df: pl.DataFrame):
        if self.master.exists():
            existing = pl.read_parquet(self.master)
            # Deduplicate by reference_number + transaction_date + bank
            combined = pl.concat([existing, new_df]).unique(
                subset=["bank", "transaction_date", "reference_number"],
                keep="last"
            )
        else:
            combined = new_df
        combined.write_parquet(self.master)

    def load_all(self) -> pl.DataFrame:
        if not self.master.exists():
            raise FileNotFoundError("No data ingested yet. Run `flow_mint ingest` first.")
        return pl.read_parquet(self.master)

    def load_month(self, bank: str, month: str) -> pl.DataFrame:
        p = self.base / bank / f"{month}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"No data for {bank}/{month}")
        return pl.read_parquet(p)

    def list_months(self) -> list:
        if not self.master.exists():
            return []
        df = pl.read_parquet(self.master)
        return sorted(
            df.with_columns(
                pl.col("transaction_date").dt.strftime("%Y-%m").alias("month")
            )["month"].unique().to_list()
        )