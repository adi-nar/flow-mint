from dataclasses import dataclass
from typing import List

@dataclass
class Domain:
    name: str
    emoji: str
    description: str
    keywords: List[str]   # matched against description (case-insensitive)

DOMAINS = [
    Domain("Housing", "🏠",
           "Rent, mortgage, maintenance, repairs, society charges",
           ["rent", "maintenance", "society", "apartment", "flat", "building"]),

    Domain("Groceries & Daily Essentials", "🛒",
           "Supermarket runs, vegetables, dairy, household consumables",
           ["grocer", "supermarket", "margin free", "zepto", "blinkit",
            "bigbasket", "daily", "vegetables", "dairy", "kirana"]),

    Domain("Dining & Takeout", "🍽️",
           "Restaurants, cafes, Swiggy, Zomato, any food ordered/eaten outside",
           ["swiggy", "zomato", "restaurant", "hotel", "cafe", "tiffin",
            "bakery", "food", "dining", "eatery", "pizza", "burger"]),

    Domain("Transport", "🚗",
           "Fuel, Ola/Uber, metro, parking, vehicle EMI, tolls",
           ["uber", "ola", "rapido", "petrol", "fuel", "toll", "parking",
            "metro", "cab", "auto", "irctc", "railway", "bus", "flight",
            "redbus", "makemytrip"]),

    Domain("Health & Medical", "🏥",
           "Doctor visits, medicines, lab tests, health insurance",
           ["apollo", "pharmacy", "pharma", "hospital", "clinic", "dental",
            "doctor", "medic", "health", "lab", "diagnostic", "insurance"]),

    Domain("Education & Learning", "🎓",
           "Courses, books, school/college fees, subscriptions like Coursera",
           ["coursera", "udemy", "school", "college", "fees", "tuition",
            "book", "education", "learning", "course"]),

    Domain("Entertainment & Leisure", "🎬",
           "Movies, events, hobbies, gaming, OTT subscriptions",
           ["netflix", "hotstar", "prime", "spotify", "youtube", "cinema",
            "movie", "game", "hobby", "entertainment", "bookmyshow", "event"]),

    Domain("Shopping & Lifestyle", "🛍️",
           "Clothing, electronics, accessories, personal care",
           ["amazon", "flipkart", "myntra", "ajio", "nykaa", "pothys",
            "shopping", "clothes", "fashion", "electronics", "decathlon"]),

    Domain("Utilities & Bills", "💡",
           "Electricity, water, gas, internet, mobile recharge, DTH",
           ["atria", "electricity", "water", "gas", "internet", "broadband",
            "recharge", "airtel", "jio", "bsnl", "dth", "bill", "paytm"]),

    Domain("EMIs & Loan Repayments", "💳",
           "Any loan installment — personal, home, vehicle, credit card",
           ["emi", "loan", "repay", "installment", "credit card"]),

    Domain("Investments & Savings", "💰",
           "SIPs, stocks, FDs, RDs, PPF — money put away intentionally",
           ["sip", "mutual fund", "zerodha", "groww", "nse", "bse",
            "investment", "fd", "fixed deposit", "ppf", "rd"]),

    Domain("Internal Transfers", "🔄",
           "Money moved between your own accounts — excluded from spend totals",
           ["neft", "imps", "self", "own account", "transfer to",
            "aditya naresh"]),   # ← personalise this

    Domain("Gifts & Donations", "🎁",
           "Gifting money, charity, temple/church donations",
           ["gift", "donation", "charity", "temple", "church", "mosque"]),

    Domain("Taxes & Fees", "🧾",
           "Advance tax, bank charges, late fees, government dues",
           ["tax", "gst", "tds", "maintenance charges", "bank charge",
            "penalty", "fine", "fee", "charges"]),

    Domain("Uncategorized", "❓",
           "Transactions that don't fit anywhere — flagged for manual review",
           []),
]

DOMAIN_NAMES = [d.name for d in DOMAINS]

def keyword_guess(description: str) -> str:
    """Rule-based fallback: returns domain name or 'Uncategorized'."""
    desc = description.lower()
    for domain in DOMAINS[:-1]:   # skip Uncategorized
        if any(kw in desc for kw in domain.keywords):
            return domain.name
    return "Uncategorized"