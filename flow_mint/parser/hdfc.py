# import polars as pl
# from .base import Parser

# class HDFCParser(Parser):
#     def __init__(self):
#         super().__init__(bank_name = "HDFC")

#     # def parse(self, file_path: str, file_type: str) -> pl.DataFrame:
#     #     if file_type.lower() in ['xls', 'xlsx']:
#     #         pdf = pl.read_excel(file_path, )
#     #     elif file_type.lower() == 'csv':
#     #         pdf = pl.read_csv(file_path)
#     #     else:
#     #         raise ValueError(f"Unsupported file type: {file_type}")
        
#     #     return pdf

#     # /Users/aditya/yard/vault/aditya/core_stuff/builds/flow_mint/parser/hdfc.py

import polars as pl
from .base import Parser

class HDFCParser(Parser):
    def __init__(self):
        super().__init__(bank_name="HDFC")

    def parse(self, file_path: str, file_type: str = None) -> pl.DataFrame:
        # Auto-detect type if not provided
        if file_type is None:
            ext = file_path.rsplit(".", 1)[-1].lower()
            file_type = ext

        if file_type.lower() in ["xls", "xlsx"]:
            raw_df = pl.read_excel(file_path, has_header=False)
            col = "column_0"
        elif file_type.lower() == "csv":
            raw_df = pl.read_csv(file_path, has_header=False, infer_schema_length=0)
            col = "column_1"
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Locate header row (row where first col == "Date")
        header_row_index = raw_df.with_row_index("idx").filter(
            pl.col(col).str.strip_chars() == "Date"
        ).select(pl.first("idx")).item()

        # Locate footer (first row of asterisks after header)
        footer_row_index = raw_df.with_row_index("idx").filter(
            (pl.col("idx") > header_row_index) &
            pl.col(col).str.starts_with("***")
        ).select(pl.first("idx")).item()

        header = raw_df.slice(header_row_index, 1)
        data = raw_df.slice(
            header_row_index + 2,
            footer_row_index - (header_row_index + 2)
        )
        data.columns = [str(v).strip() if v else f"col_{i}"
                        for i, v in enumerate(header.row(0))]

        # Handle multi-line narrations (XLS only; CSV is already clean)
        df_processed = (
            data
            .with_columns(
                transaction_group=pl.col("Date").is_not_null().cum_sum()
            )
            .group_by("transaction_group")
            .agg(
                pl.col("Date").first(),
                pl.col("Chq./Ref.No.").first(),
                pl.col("Value Dt").first(),
                pl.col("Narration")
                  .filter(pl.col("Narration").is_not_null())
                  .str.concat(" "),
                pl.col("Withdrawal Amt.").first(),
                pl.col("Deposit Amt.").first(),
                pl.col("Closing Balance").last(),
            )
            .drop("transaction_group")
        )

        return (
            df_processed
            .rename({
                "Date": "transaction_date",
                "Narration": "description",
                "Chq./Ref.No.": "reference_number",
                "Value Dt": "value_date",
                "Withdrawal Amt.": "debit",
                "Deposit Amt.": "credit",
                "Closing Balance": "balance",
            })
            .with_columns([
                pl.col("transaction_date").str.to_date("%d/%m/%y", strict=False),
                pl.col("value_date").str.to_date("%d/%m/%y", strict=False),
                pl.col(["debit", "credit", "balance"])
                  .str.replace_all(",", "")
                  .cast(pl.Float64, strict=False),
            ])
            .with_columns(pl.lit("HDFC").alias("bank"))
            .select([
                "bank", "transaction_date", "value_date",
                "description", "debit", "credit",
                "balance", "reference_number",
            ])
        )