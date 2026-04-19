import polars as pl
from .base import Parser

class CUBParser(Parser):
    def __init__(self):
        super().__init__(bank_name="CUB")

    def parse(self, file_path: str, file_type: str = None) -> pl.DataFrame:
        if file_type is None:
            ext = file_path.rsplit(".", 1)[-1].lower()
            file_type = ext

        if file_type.lower() in ["xls", "xlsx"]:
            raw_df = pl.read_excel(file_path, has_header=False)
            col = "column_1"
        elif file_type.lower() == "csv":
            raw_df = pl.read_csv(file_path, has_header=False, infer_schema_length=0)
            col = "column_1"
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Find the header row (contains "DATE" or "Date")
        header_row_index = raw_df.with_row_index("idx").filter(
            pl.col(col).str.strip_chars().str.to_uppercase() == "DATE"
        ).select(pl.first("idx")).item()

        header = raw_df.slice(header_row_index, 1)
        data_raw = raw_df.slice(header_row_index + 1, len(raw_df))

        # Rename columns from header row
        data_raw.columns = [str(v).strip() if v else f"col_{i}"
                            for i, v in enumerate(header.row(0))]

        # Drop rows where DATE is null or starts with whitespace-only / "TOTAL" / "END"
        data = data_raw.filter(
            pl.col("DATE").is_not_null() &
            ~pl.col("DATE").str.strip_chars().str.to_uppercase()
              .is_in(["", "TOTAL", "*", "END OF STATEMENT"])
        )

        # Clean amount columns: strip whitespace, cast to float
        def clean_amount(col_name: str):
            return (
                pl.col(col_name)
                  .str.strip_chars()
                  .str.replace_all(",", "")
                  .cast(pl.Float64, strict=False)
            )

        return (
            data
            .rename({
                "DATE": "transaction_date",
                "DESCRIPTION": "description",
                "CHEQUE NO": "reference_number",
                "DEBIT": "debit",
                "CREDIT": "credit",
                "BALANCE": "balance",
            })
            .with_columns([
                pl.col("transaction_date").str.strip_chars()
                  .str.to_date("%d/%m/%Y", strict=False),
                clean_amount("debit"),
                clean_amount("credit"),
                clean_amount("balance"),
                pl.lit(None).cast(pl.Date).alias("value_date"),
                pl.lit("CUB").alias("bank"),
            ])
            .select([
                "bank", "transaction_date", "value_date",
                "description", "debit", "credit",
                "balance", "reference_number",
            ])
        )