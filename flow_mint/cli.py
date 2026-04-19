"""
flow_mint CLI

Commands:
  ingest      Parse a bank statement and store it
  categorize  Categorize transactions (keyword by default, --ai for Claude)
  report      Print monthly summary
  trends      Show month-over-month trends
  suggest     Get savings suggestions (rule-based by default, --ai for Claude)
  list        List ingested months

Usage examples (no API key needed):
  python -m flow_mint.cli ingest --bank HDFC --file ~/Downloads/hdfc.csv
  python -m flow_mint.cli ingest --bank CUB  --file ~/Downloads/cub.xls
  python -m flow_mint.cli categorize
  python -m flow_mint.cli report --month 2025-09
  python -m flow_mint.cli trends
  python -m flow_mint.cli suggest --month 2025-09
  python -m flow_mint.cli list

Usage examples (with Claude AI, requires ANTHROPIC_API_KEY):
  python -m flow_mint.cli categorize --ai
  python -m flow_mint.cli suggest --month 2025-09 --ai
"""

import argparse, sys
from pathlib import Path
import polars as pl

from .parser import get_parser
from .parser.converter import convert_xls_to_csv
from .categorizer.ai import add_categories
from .categorizer.domains import DOMAIN_NAMES
from .analyzer.flow import monthly_summary, detect_internal_transfers
from .analyzer.trends import month_over_month, top_categories
from .storage.store import ParquetStore
from .advisor.suggest import generate_suggestions

STORE = ParquetStore("data")

# ── helpers ────────────────────────────────────────────────

def _infer_month(df: pl.DataFrame) -> str:
    return df["transaction_date"].dt.strftime("%Y-%m").mode().first()

def _print_table(df: pl.DataFrame, max_rows: int = 50):
    with pl.Config(tbl_rows=max_rows, tbl_width_chars=160):
        print(df)

# ── commands ───────────────────────────────────────────────

def cmd_ingest(args):
    file_path = args.file
    bank = args.bank.upper()
    ext = Path(file_path).suffix.lower().lstrip(".")

    # Auto-convert XLS → CSV first
    if ext in ("xls", "xlsx"):
        print(f"🔄 Converting {ext.upper()} → CSV …")
        file_path = convert_xls_to_csv(file_path)
        ext = "csv"
        print(f"   Saved as: {file_path}")

    parser = get_parser(bank)
    print(f"📥 Parsing {bank} statement …")
    df = parser.parse(file_path, ext)
    print(f"   {len(df)} transactions found.")

    # Auto-detect internal transfers before AI categorization
    df = df.with_columns(pl.lit("Uncategorized").alias("category"),
                         pl.lit("low").alias("confidence"),
                         pl.lit(False).alias("category_confirmed"))
    df = detect_internal_transfers(df)

    month = _infer_month(df)
    STORE.save(df, bank, month)
    print(f"✅ Saved {bank} / {month} → data/{bank}/{month}.parquet")
    print("   Run `flow_mint categorize` to assign categories.")


def cmd_categorize(args):
    df = STORE.load_all()
    unconfirmed = df.filter(~pl.col("category_confirmed"))

    if len(unconfirmed) == 0:
        print("✅ All transactions already confirmed.")
        return

    use_ai = getattr(args, "ai", False)
    mode = "Claude AI" if use_ai else "keyword matching"
    print(f"\n🔍 Categorizing {len(unconfirmed)} transactions using {mode} …\n")
    unconfirmed = add_categories(unconfirmed, use_ai=use_ai)

    confirmed_rows = []
    for row in unconfirmed.iter_rows(named=True):
        desc  = row["description"][:60]
        cat   = row["category"]
        conf  = row["confidence"]
        amount = row["debit"] or row["credit"]
        sign   = "-" if row["debit"] else "+"

        print(f"\n  {sign}₹{amount:<10.2f}  {desc}")
        print(f"  AI suggests: [{cat}] (confidence: {conf})")
        user = input("  Accept? (Enter=yes / type new category / s=skip): ").strip()

        if user.lower() == "s":
            row["category_confirmed"] = False
        elif user == "":
            row["category_confirmed"] = True
        elif user in DOMAIN_NAMES:
            row["category"] = user
            row["category_confirmed"] = True
        else:
            # Fuzzy match attempt
            matches = [d for d in DOMAIN_NAMES if user.lower() in d.lower()]
            if len(matches) == 1:
                print(f"  → Matched to: {matches[0]}")
                row["category"] = matches[0]
                row["category_confirmed"] = True
            elif len(matches) > 1:
                print(f"  Ambiguous. Did you mean: {matches}")
                row["category_confirmed"] = False
            else:
                print(f"  Unknown category — keeping AI suggestion.")
                row["category_confirmed"] = True

        confirmed_rows.append(row)

    updated = pl.DataFrame(confirmed_rows, schema=unconfirmed.schema)

    # Merge back with already-confirmed rows
    already_confirmed = df.filter(pl.col("category_confirmed"))
    final = pl.concat([already_confirmed, updated]).sort("transaction_date")

    # Re-save master
    final.write_parquet(STORE.master)
    confirmed_count = updated.filter(pl.col("category_confirmed")).height
    print(f"\n✅ {confirmed_count}/{len(updated)} transactions confirmed.")


def cmd_report(args):
    df = STORE.load_all()
    if args.month:
        df = df.filter(pl.col("transaction_date").dt.strftime("%Y-%m") == args.month)

    print(f"\n📊 MONTHLY SUMMARY\n{'─'*60}")
    summary = monthly_summary(df)
    _print_table(summary)

    print(f"\n🏆 TOP SPENDING CATEGORIES\n{'─'*60}")
    _print_table(top_categories(df))


def cmd_trends(args):
    df = STORE.load_all()
    summary = monthly_summary(df)
    trends = month_over_month(summary)
    print(f"\n📈 MONTH-OVER-MONTH TRENDS\n{'─'*60}")
    _print_table(trends)


def cmd_suggest(args):
    month  = args.month if hasattr(args, "month") else None
    use_ai = getattr(args, "ai", False)
    mode   = "Claude AI" if use_ai else "rule-based"
    print(f"\n💡 SAVINGS SUGGESTIONS ({mode}){' for ' + month if month else ''}\n{'─'*60}")
    print(generate_suggestions(df, month=month, use_ai=use_ai))


def cmd_list(args):
    months = STORE.list_months()
    if not months:
        print("No data ingested yet.")
    else:
        print("Ingested months:")
        for m in months:
            print(f"  • {m}")


# ── main ───────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(prog="flow_mint", description="Personal finance tracker")
    sub = p.add_subparsers(dest="command")

    # ingest
    pi = sub.add_parser("ingest", help="Parse and store a bank statement")
    pi.add_argument("--bank", required=True, help="Bank name (HDFC, CUB)")
    pi.add_argument("--file", required=True, help="Path to statement (CSV or XLS)")

    # categorize
    pc = sub.add_parser("categorize", help="Categorize transactions (keyword by default)")
    pc.add_argument("--ai", action="store_true",
                    help="Use Claude AI instead of keyword matching (needs ANTHROPIC_API_KEY)")

    # report
    pr = sub.add_parser("report", help="Print monthly summary")
    pr.add_argument("--month", help="Filter to YYYY-MM (e.g. 2025-09)")

    # trends
    sub.add_parser("trends", help="Month-over-month trends")

    # suggest
    ps = sub.add_parser("suggest", help="Get savings suggestions")
    ps.add_argument("--month", help="Focus on YYYY-MM")
    ps.add_argument("--ai", action="store_true",
                    help="Use Claude AI instead of rule-based tips (needs ANTHROPIC_API_KEY)")

    # list
    sub.add_parser("list", help="List ingested months")

    args = p.parse_args()
    dispatch = {
        "ingest":     cmd_ingest,
        "categorize": cmd_categorize,
        "report":     cmd_report,
        "trends":     cmd_trends,
        "suggest":    cmd_suggest,
        "list":       cmd_list,
    }

    if args.command not in dispatch:
        p.print_help()
        sys.exit(1)

    dispatch[args.command](args)

if __name__ == "__main__":
    main()

