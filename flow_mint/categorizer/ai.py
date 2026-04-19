import os, json
from typing import List, Dict
import polars as pl
from .domains import DOMAIN_NAMES, keyword_guess

SYSTEM_PROMPT = f"""You are a transaction categorizer for personal finance.
Given a list of bank transaction descriptions, assign each one exactly ONE category
from this list:

{chr(10).join(f'- {d}' for d in DOMAIN_NAMES)}

Rules:
- "Internal Transfers" = money moving between the user's OWN accounts (NEFT/IMPS to self, same-name beneficiary).
- "Investments & Savings" = SIPs, mutual funds, FDs — money deliberately set aside.
- When in doubt, prefer a specific category over "Uncategorized".

Respond ONLY with a JSON array of objects, one per transaction, in the same order:
[{{"id": <index>, "category": "<category name>", "confidence": "high|medium|low"}}]
No markdown, no extra text."""

def _get_client():
    """Lazily load Anthropic client. Returns None if unavailable."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=key)
    except ImportError:
        return None

def categorize_batch_keyword(descriptions: List[str]) -> List[Dict]:
    """Pure keyword-based categorization. Always available, no API needed."""
    return [
        {"id": i, "category": keyword_guess(d), "confidence": "keyword"}
        for i, d in enumerate(descriptions)
    ]

def categorize_batch_ai(descriptions: List[str], batch_size: int = 50) -> List[Dict]:
    """
    Claude API categorization in batches.
    Raises RuntimeError if API key is missing or anthropic not installed.
    """
    client = _get_client()
    if client is None:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set or `anthropic` package not installed.\n"
            "Run without --ai flag to use keyword-based categorization."
        )

    results = []
    for start in range(0, len(descriptions), batch_size):
        batch = descriptions[start:start + batch_size]
        numbered = "\n".join(f"{start+i}: {d}" for i, d in enumerate(batch))
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": numbered}],
        )
        raw = msg.content[0].text.strip()
        parsed = json.loads(raw)
        results.extend(parsed)

    return results

def add_categories(df: pl.DataFrame, use_ai: bool = False) -> pl.DataFrame:
    """
    Adds 'category', 'confidence', and 'category_confirmed' columns.

    Args:
        df:      transactions DataFrame
        use_ai:  if True, uses Claude API; if False, uses keyword matching
    """
    descriptions = df["description"].to_list()

    if use_ai:
        print("🤖 Using Claude AI for categorization …")
        cats = categorize_batch_ai(descriptions)
    else:
        print("🔑 Using keyword-based categorization …")
        cats = categorize_batch_keyword(descriptions)

    cats_sorted = sorted(cats, key=lambda x: x["id"])
    return df.with_columns([
        pl.Series("category",   [c["category"]   for c in cats_sorted]),
        pl.Series("confidence", [c["confidence"] for c in cats_sorted]),
        pl.lit(False).alias("category_confirmed"),
    ])