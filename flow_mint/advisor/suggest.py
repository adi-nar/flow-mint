import os
from typing import Optional
import polars as pl
from ..analyzer.flow import monthly_summary, split_flows
from ..analyzer.trends import top_categories

# ── Rule-based suggestions (no AI needed) ──────────────────

def _rule_based_suggestions(df: pl.DataFrame) -> str:
    """
    Generates savings suggestions using pure arithmetic + heuristics.
    No API key required.
    """
    flows   = split_flows(df)
    summary = monthly_summary(df)
    top     = top_categories(df, n=5)

    total_in  = flows["influx"]["credit"].sum() or 0
    total_out = flows["outflow"]["debit"].sum() or 0
    net       = total_in - total_out
    savings_rate = (net / total_in * 100) if total_in else 0

    lines = [
        "💡 SAVINGS SUGGESTIONS (rule-based)\n" + "─" * 50,
        f"\n📥 Total Influx  : ₹{total_in:,.2f}",
        f"📤 Total Outflow : ₹{total_out:,.2f}",
        f"💰 Net Savings   : ₹{net:,.2f}  ({savings_rate:.1f}% savings rate)\n",
    ]

    # Suggestion 1: top spending category
    if len(top) > 0:
        top_cat   = top.row(0, named=True)
        top_name  = top_cat["category"]
        top_spent = top_cat["total_spent"]
        pct       = top_spent / total_out * 100 if total_out else 0
        lines.append(
            f"1️⃣  Your biggest expense category is [{top_name}] at "
            f"₹{top_spent:,.2f} ({pct:.1f}% of total spend). "
            f"Look for one recurring charge here you can cut or reduce."
        )

    # Suggestion 2: savings rate target
    target_rate = 30.0
    target_save = total_in * target_rate / 100
    if savings_rate < target_rate:
        shortfall = target_save - net
        lines.append(
            f"\n2️⃣  A healthy savings rate is ~{target_rate:.0f}%. "
            f"You're at {savings_rate:.1f}%. "
            f"Reducing spend by ₹{shortfall:,.2f} next month gets you there."
        )
    else:
        lines.append(
            f"\n2️⃣  Great job! Your {savings_rate:.1f}% savings rate exceeds "
            f"the {target_rate:.0f}% target. Try increasing SIP/investment contributions."
        )

    # Suggestion 3: uncategorized spend flag
    uncat_df = df.filter(
        (pl.col("category") == "Uncategorized") &
        pl.col("debit").is_not_null()
    )
    if len(uncat_df) > 0:
        uncat_total = uncat_df["debit"].sum()
        lines.append(
            f"\n3️⃣  ₹{uncat_total:,.2f} across {len(uncat_df)} transactions is "
            f"still 'Uncategorized'. Run `flow_mint categorize` to tag them — "
            f"hidden spend is hard to control."
        )

    # Suggestion 4: dining/takeout check
    dining_df = df.filter(
        (pl.col("category") == "Dining & Takeout") &
        pl.col("debit").is_not_null()
    )
    if len(dining_df) > 0:
        dining_total = dining_df["debit"].sum()
        dining_pct   = dining_total / total_out * 100 if total_out else 0
        if dining_pct > 15:
            lines.append(
                f"\n4️⃣  Dining & Takeout is ₹{dining_total:,.2f} "
                f"({dining_pct:.1f}% of spend). Cooking at home 2–3 more "
                f"times a week could save ₹{dining_total*0.3:,.0f}+ next month."
            )

    lines.append(
        f"\n💡 Tip: Run with --ai flag once you have an Anthropic API key "
        f"for deeper, personalised suggestions."
    )
    return "\n".join(lines)


# ── AI suggestions ──────────────────────────────────────────

def _ai_suggestions(df: pl.DataFrame) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set. Run without --ai to use rule-based suggestions."
        )
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
    except ImportError:
        raise RuntimeError("Install the `anthropic` package: pip install anthropic")

    summary  = monthly_summary(df)
    top_cats = top_categories(df, n=8)
    flows    = split_flows(df)

    total_in  = flows["influx"]["credit"].sum() or 0
    total_out = flows["outflow"]["debit"].sum() or 0

    prompt = f"""Here is my personal finance data:

MONTHLY SUMMARY:
{summary.to_pandas().to_string(index=False)}

TOP SPENDING CATEGORIES:
{top_cats.to_pandas().to_string(index=False)}

Total influx: ₹{total_in:,.2f}
Total outflow: ₹{total_out:,.2f}
Net savings: ₹{total_in - total_out:,.2f}

Based on this, give me:
1. Three specific, actionable ways to save more next month.
2. One category where spending looks unusually high and why.
3. A realistic savings target for next month.
4. One positive observation about my spending habits.

Be concise and specific. Use ₹ for amounts."""

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


# ── Public API ──────────────────────────────────────────────

def generate_suggestions(
    df: pl.DataFrame,
    month: Optional[str] = None,
    use_ai: bool = False,
) -> str:
    if month:
        df = df.filter(pl.col("transaction_date").dt.strftime("%Y-%m") == month)

    if use_ai:
        return _ai_suggestions(df)
    return _rule_based_suggestions(df)

