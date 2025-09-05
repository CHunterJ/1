from pathlib import Path
import polars as pl
import sys

# Absolute path to your unzipped data
CORPUS_DIR = Path("/Users/christopherjorgensen/Downloads/Corpus")

# Find parquet files (adjust to "*.csv" if needed)
paths = [str(p) for p in CORPUS_DIR.rglob("*.parquet")]
if not paths:
    print(f"No Parquet files found under {CORPUS_DIR}. Check the path or extension.")
    sys.exit(1)

# Lazy scan
lf = pl.scan_parquet(paths)

# Get schema without the warning
schema = lf.collect_schema()
print("Schema:", schema)

# Quick row count (streaming)
print("Row count:", lf.select(pl.len()).collect(streaming=True))

# Peek a few rows
print(lf.limit(3).collect(streaming=True))