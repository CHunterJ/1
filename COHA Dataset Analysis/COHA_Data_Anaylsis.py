from pathlib import Path
import polars as pl

BASE = Path("/Users/christopherjorgensen/Downloads/Corpus")
paths = list(BASE.rglob("*.parquet"))

def has_any(schema, names):
    n = [n.lower() for n in schema.names()]
    return any(x in n for x in names)

for p in paths[:200]:  # scan a subset; bump if needed
    try:
        sch = pl.scan_parquet(str(p)).collect_schema()
        if has_any(sch, ["text","body","content","full_text"]):
            print("FULL-TEXT CANDIDATE:", p, sch)
        if has_any(sch, ["lemma","word","pos","tag"]):
            print("WORDS/LEXICON CANDIDATE:", p, sch)
        if has_any(sch, ["year","decade","genre","section"]):
            print("TEXT METADATA CANDIDATE:", p, sch)
    except Exception:
        pass