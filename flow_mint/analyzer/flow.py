import polars as pl

INTERNAL_TRANSFER_CATEGORY = "Internal Transfers"

def split_flows(df: pl.DataFrame) -> dict:
    """
    Splits a transactions DataFrame into:
      - influx:    confirmed credits (salary, refunds, etc.)
      - outflow:   confirmed debits (spending)
      - internal:  transfers between own accounts
    """
    internal = df.filter(pl.col("category") == INTERNAL_TRANSFER_CATEGORY)
    real = df.filter(pl.col("category") != INTERNAL_TRANSFER_CATEGORY)

    influx  = real.filter(pl.col("credit").is_not_null() & (pl.col("credit") > 0))
    outflow = real.filter(pl.col("debit").is_not_null()  & (pl.col("debit")  > 0))

    return {"influx": influx, "outflow": outflow, "internal": internal}

def monthly_summary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Returns a per-month summary with total influx, outflow, internal,
    net savings, and per-category outflow breakdown.
    """
    df = df.with_columns(
        pl.col("transaction_date").dt.strftime("%Y-%m").alias("month")
    )

    flows = split_flows(df)

    influx_mo = (
        flows["influx"]
        .group_by("month")
        .agg(pl.col("credit").sum().alias("total_influx"))
    )
    outflow_mo = (
        flows["outflow"]
        .group_by(["month", "category"])
        .agg(pl.col("debit").sum().alias("amount"))
        .pivot(on="category", index="month", values="amount", aggregate_function="sum")
    )
    internal_mo = (
        flows["internal"]
        .group_by("month")
        .agg(
            (pl.col("debit").sum() + pl.col("credit").sum()).alias("total_internal")
        )
    )

    summary = (
        influx_mo
        .join(outflow_mo, on="month", how="outer_coalesce")
        .join(internal_mo, on="month", how="outer_coalesce")
        .sort("month")
    )

    # Add net savings column
    category_cols = [c for c in summary.columns
                     if c not in ("month", "total_influx", "total_internal")]
    summary = summary.with_columns(
        (pl.col("total_influx") -
         pl.sum_horizontal([pl.col(c).fill_null(0) for c in category_cols])
        ).alias("net_savings")
    )
    return summary

def detect_internal_transfers(df: pl.DataFrame) -> pl.DataFrame:
    """
    Auto-detect internal transfers by matching UPI reference numbers
    that appear as both debit in one bank and credit in another.
    Updates category to 'Internal Transfers' for matched rows.
    """
    # Extract reference number from description where possible
    ref_debit  = df.filter(pl.col("debit").is_not_null())["reference_number"].drop_nulls()
    ref_credit = df.filter(pl.col("credit").is_not_null())["reference_number"].drop_nulls()

    shared_refs = set(ref_debit.to_list()) & set(ref_credit.to_list())

    if shared_refs:
        df = df.with_columns(
            pl.when(pl.col("reference_number").is_in(list(shared_refs)))
              .then(pl.lit(INTERNAL_TRANSFER_CATEGORY))
              .otherwise(pl.col("category"))
              .alias("category")
        )
    return df