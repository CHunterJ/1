import polars as pl
from pathlib import Path
from glob import glob

# Find one lexicon file
lex_files = glob(str(Path.home() / "Downloads/Corpus/**/*Word*/*.parquet"), recursive=True)
print("Found", len(lex_files), "lexicon candidates")
print("Example file:", lex_files[0])

# Show its schema
df = pl.read_parquet(lex_files[0], n_rows=0)  # just read schema
print(df.schema)