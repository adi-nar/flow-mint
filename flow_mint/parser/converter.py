from pathlib import Path
import subprocess, shutil

def convert_xls_to_csv(xls_path: str, out_dir: str = None) -> str:
    """
    Converts an XLS/XLSX file to CSV using python-calamine (via polars).
    Returns the path to the generated CSV.
    """
    import polars as pl
    p = Path(xls_path)
    out = Path(out_dir) if out_dir else p.parent
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / (p.stem + ".csv")

    df = pl.read_excel(xls_path, has_header=False, infer_schema_length=0)
    df.write_csv(csv_path)
    return str(csv_path)


def convert_xls_to_parquet(xls_path: str, out_dir: str = None) -> str:
    """
    Converts an XLS/XLSX file to Parquet (raw, no parsing).
    """
    import polars as pl
    p = Path(xls_path)
    out = Path(out_dir) if out_dir else p.parent
    out.mkdir(parents=True, exist_ok=True)
    pq_path = out / (p.stem + ".parquet")

    df = pl.read_excel(xls_path, has_header=False, infer_schema_length=0)
    df.write_parquet(pq_path)
    return str(pq_path)