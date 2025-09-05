from pathlib import Path
import polars as pl

BASE = Path("/Users/christopherjorgensen/Downloads/Corpus")

for p in BASE.rglob("*.parquet"):
    try:
        sch = pl.scan_parquet(str(p)).collect_schema()
        if any(c.lower() in ("text","body","content","full_text") for c in sch.names()):
            print("FULL TEXT FOUND IN:", p, sch)
    except:
        pass