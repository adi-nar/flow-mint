from .hdfc import HDFCParser
from .cub import CUBParser
from .converter import convert_xls_to_csv, convert_xls_to_parquet

PARSERS = {
    "HDFC": HDFCParser,
    "CUB": CUBParser,
}

def get_parser(bank_name: str):
    bank = bank_name.upper()
    if bank not in PARSERS:
        raise ValueError(f"No parser for bank: {bank_name}. Available: {list(PARSERS)}")
    return PARSERS[bank]()
