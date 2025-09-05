# COHA_Analysis.py
from pathlib import Path
import polars as pl
import sys

# Initialize lazy frames to avoid NameError before discovery
lf_tokens = None
lf_words = None
lf_texts = None

# ---- CONFIG: set this to the parent folder holding the three subfolders ----
BASE = Path("/Users/christopherjorgensen/Downloads").resolve()

# Debug: show immediate subfolders and parquet counts to verify layout
print("BASE:", BASE)
for name in ["Corpus", "Sources", "Text"]:
    p = BASE / name
    print(f"Exists {name}? ", p.exists(), " Parquet files: ", len(list(p.rglob("*.parquet"))) if p.exists() else 0)
# Also check for any folder starting with 'Word'
word_dirs = [d for d in BASE.iterdir() if d.is_dir() and d.name.lower().startswith("word")]
print("Word-like dirs:", [d.name for d in word_dirs])
for d in word_dirs:
    try:
        print("  ", d, "parquet files:", len(list(d.rglob("*.parquet"))))
    except Exception:
        pass

TOKENS_GLOB = str(BASE / "Corpus/**/*.parquet")
WORDS_GLOB  = str(BASE / "Word*/**/*.parquet")     # matches "Word/lemma/POS" variations
TEXTS_GLOB1 = str(BASE / "Sources/**/*.parquet")
TEXTS_GLOB2 = str(BASE / "Text/**/*.parquet")      # some exports call it "Text"

# Also look for words/texts nested inside the Corpus folder
WORDS_GLOB_IN_CORPUS  = str(BASE / "Corpus/**/Word*/**/*.parquet")
TEXTS_GLOB_IN_CORPUS1 = str(BASE / "Corpus/**/Sources/**/*.parquet")
TEXTS_GLOB_IN_CORPUS2 = str(BASE / "Corpus/**/Text/**/*.parquet")

def must_have(paths, label):
    files = list(Path().glob(paths)) if isinstance(paths, str) else []
    return files

# Build lazy frames (explicit globs) — supports absolute patterns
from glob import glob

def scan_union(glob_pattern_list):
    files = []
    for patt in glob_pattern_list:
        files.extend([Path(p) for p in glob(patt, recursive=True)])
    if not files:
        return None
    lfs = [pl.scan_parquet(str(f)) for f in files]
    return lfs[0] if len(lfs) == 1 else pl.concat(lfs, how="vertical_relaxed")


print("Matched token files:", len([p for p in glob(TOKENS_GLOB, recursive=True)]))
print("Matched word files :", len([p for p in glob(WORDS_GLOB,  recursive=True)]) + len([p for p in glob(WORDS_GLOB_IN_CORPUS,  recursive=True)]))
print("Matched text files :", len([p for p in glob(TEXTS_GLOB1, recursive=True)]) + len([p for p in glob(TEXTS_GLOB2, recursive=True)]) + len([p for p in glob(TEXTS_GLOB_IN_CORPUS1, recursive=True)]) + len([p for p in glob(TEXTS_GLOB_IN_CORPUS2, recursive=True)]))

# ---------- Primary discovery via schema classification ----------
# Gather all parquet (& csv) under BASE/Corpus and classify by columns
corpus_parquets = [Path(p) for p in glob(str(BASE / "Corpus/**/*.parquet"), recursive=True)]
corpus_csvs     = [Path(p) for p in glob(str(BASE / "Corpus/**/*.csv"), recursive=True)]

def get_schema_names(path):
    try:
        if path.suffix.lower() == ".csv":
            lf = pl.scan_csv(str(path), has_header=True, infer_schema_length=1000)
        else:
            lf = pl.scan_parquet(str(path))
        return [c.lower() for c in lf.collect_schema().names()], lf
    except Exception:
        return [], None

token_lfs, word_lfs, text_lfs = [], [], []

for p in corpus_parquets + corpus_csvs:
    names, lf = get_schema_names(p)
    if not names or lf is None:
        continue
    has_textid = any(n in names for n in ["textid","text_id","docid","doc_id"])
    has_wordid = any(n in names for n in ["wordid","word_id"])
    has_wordish = any(n in names for n in ["word","token","form","lemma","pos","upos","xpos","tag"])
    has_yearish = any(n in names for n in ["year","date_year","decade","genre","section"])

    # Classify:
    # tokens: textID + wordID present, and no word/lemma/pos columns
    if has_textid and has_wordid and not (has_wordish or has_yearish):
        token_lfs.append(lf)
        continue
    # words/lexicon: wordID + (word/lemma/pos) present, and no textID
    if has_wordid and (has_wordish) and not has_textid:
        word_lfs.append(lf)
        continue
    # texts/metadata: textID + (year/genre/decade) present, and no wordID
    if has_textid and has_yearish and not has_wordid:
        text_lfs.append(lf)
        continue

# Build normalized lazy frames from classified lists (select/cast to align schemas)
def build_tokens_lazy(lfs):
    if not lfs:
        return None
    norm = []
    for lf in lfs:
        sch = lf.collect_schema()
        cols = {c.lower(): c for c in sch.names()}
        exprs = []
        # textID
        for k in ("textid","text_id","docid","doc_id"):
            if k in cols:
                exprs.append(pl.col(cols[k]).cast(pl.Int64).alias("textID"))
                break
        # wordID
        for k in ("wordid","word_id"):
            if k in cols:
                exprs.append(pl.col(cols[k]).cast(pl.Int64).alias("wordID"))
                break
        # optional occurrence id
        if "id" in cols:
            exprs.append(pl.col(cols["id"]).cast(pl.Int64).alias("occID"))
        norm.append(lf.select(exprs))
    return norm[0] if len(norm)==1 else pl.concat(norm, how="vertical_relaxed")

def build_words_lazy(lfs):
    if not lfs:
        return None
    norm = []
    for lf in lfs:
        sch = lf.collect_schema()
        cols = {c.lower(): c for c in sch.names()}
        exprs = []
        # wordID
        key = cols.get("wordid", cols.get("word_id"))
        if key:
            exprs.append(pl.col(key).cast(pl.Int64).alias("wordID"))
        # surface word
        for k in ("word","token","form"):
            if k in cols:
                exprs.append(pl.col(cols[k]).cast(pl.Utf8).alias("word"))
                break
        # lemma
        if "lemma" in cols:
            exprs.append(pl.col(cols["lemma"]).cast(pl.Utf8).alias("lemma"))
        # pos
        for k in ("pos","upos","xpos","tag"):
            if k in cols:
                exprs.append(pl.col(cols[k]).cast(pl.Utf8).alias("pos"))
                break
        norm.append(lf.select(exprs))
    return norm[0] if len(norm)==1 else pl.concat(norm, how="vertical_relaxed")

def build_texts_lazy(lfs):
    if not lfs:
        return None
    norm = []
    for lf in lfs:
        sch = lf.collect_schema()
        cols = {c.lower(): c for c in sch.names()}
        exprs = []
        # textID
        for k in ("textid","text_id","docid","doc_id"):
            if k in cols:
                exprs.append(pl.col(cols[k]).cast(pl.Int64).alias("textID"))
                break
        # year or date_year
        if "year" in cols:
            exprs.append(pl.col(cols["year"]).cast(pl.Int32).alias("year"))
        elif "date_year" in cols:
            exprs.append(pl.col(cols["date_year"]).cast(pl.Int32).alias("year"))
        # decade (optional)
        if "decade" in cols:
            exprs.append(pl.col(cols["decade"]).cast(pl.Int32).alias("decade"))
        # genre/section (optional)
        for k in ("genre","section"):
            if k in cols:
                exprs.append(pl.col(cols[k]).cast(pl.Utf8).alias("genre"))
                break
        norm.append(lf.select(exprs))
    return norm[0] if len(norm)==1 else pl.concat(norm, how="vertical_relaxed")

lf_tokens = build_tokens_lazy(token_lfs)
_tmp_words = build_words_lazy(word_lfs)
if _tmp_words is not None:
    lf_words = _tmp_words  # prefer classified/normalized words if found
lf_texts  = build_texts_lazy(text_lfs)

# Print normalized schemas to verify shapes before proceeding
if lf_tokens is not None:
    print("Normalized TOKENS schema:", lf_tokens.collect_schema())
if lf_words is not None:
    print("Normalized WORDS schema:", lf_words.collect_schema())
if lf_texts is not None:
    print("Normalized TEXTS schema:", lf_texts.collect_schema())

print(f"Classified token shards: {len(token_lfs)}, words shards: {len(word_lfs)}, texts shards: {len(text_lfs)}")

# Targeted fallback: if no words shards were classified, but a typical lexicon folder exists, force-load it.
if lf_words is None:
    forced_word_dir = BASE / "Corpus" / "Word_lemma_PoS"
    if forced_word_dir.exists():
        from glob import glob as _g
        _forced_paths = _g(str(forced_word_dir / "**/*.parquet"), recursive=True)
        if _forced_paths:
            print(f"Forcing words/lexicon from {forced_word_dir} with {len(_forced_paths)} shards")
            lf_words = pl.concat([pl.scan_parquet(p) for p in _forced_paths], how="vertical_relaxed")
            # Print a preview of the inferred schema to confirm column names
            try:
                _sch = pl.scan_parquet(_forced_paths[0]).collect_schema()
                print("Forced lexicon sample schema:", _sch)
            except Exception:
                pass

 # ---------- Secondary fallback (project-wide) if classification missed files ----------
from itertools import chain
if (lf_words is None) or (lf_texts is None):
    all_parquets = [Path(p) for p in glob(str(BASE / "**/*.parquet"), recursive=True)]
    all_csvs     = [Path(p) for p in glob(str(BASE / "**/*.csv"),     recursive=True)]
else:
    all_parquets, all_csvs = [], []

def try_collect_schema(p):
    try:
        return pl.scan_parquet(str(p)).collect_schema().names()
    except Exception:
        return []

# Detect words/lexicon: must have wordID AND (word OR lemma OR pos)
if lf_words is None:
    words_cands = []
    for p in all_parquets:
        names = [n.lower() for n in try_collect_schema(p)]
        if not names:
            continue
        has_wordid = any(n in names for n in ["wordid","word_id"])
        has_wordish = any(n in names for n in ["word","token","form","lemma","pos","upos","xpos","tag"])
        has_textid  = any(n in names for n in ["textid","text_id","docid","doc_id"])
        if has_wordid and has_wordish and not has_textid:
            words_cands.append(p)
    if not words_cands:
        # scan CSV candidates by schema-like header
        csv_words = []
        for p in all_csvs:
            try:
                lf = pl.scan_csv(str(p), has_header=True, infer_schema_length=1000)
                names = [n.lower() for n in lf.collect_schema().names()]
            except Exception:
                continue
            has_wordid = any(n in names for n in ["wordid","word_id"])
            has_wordish = any(n in names for n in ["word","token","form","lemma","pos","upos","xpos","tag"])
            has_textid  = any(n in names for n in ["textid","text_id","docid","doc_id"])
            if has_wordid and has_wordish and not has_textid:
                csv_words.append(lf)
        if csv_words:
            print(f"Fallback found words/lexicon CSV shards: {len(csv_words)}")
            lf_words = pl.concat(csv_words, how="vertical_relaxed")
    if words_cands:
        print(f"Fallback found words/lexicon shards: {len(words_cands)}")
        lf_words = pl.concat([pl.scan_parquet(str(f)) for f in words_cands], how="vertical_relaxed")

# Detect texts/metadata: must have textID AND (year/decade/genre)
if lf_texts is None:
    texts_cands = []
    for p in all_parquets:
        names = [n.lower() for n in try_collect_schema(p)]
        if not names:
            continue
        has_textid = any(n in names for n in ["textid","text_id","docid","doc_id"])
        has_yearish = any(n in names for n in ["year","date_year","decade","genre","section"])
        has_wordid  = any(n in names for n in ["wordid","word_id"])
        if has_textid and has_yearish and not has_wordid:
            texts_cands.append(p)
    if not texts_cands:
        csv_texts = []
        for p in all_csvs:
            try:
                lf = pl.scan_csv(str(p), has_header=True, infer_schema_length=1000)
                names = [n.lower() for n in lf.collect_schema().names()]
            except Exception:
                continue
            has_textid = any(n in names for n in ["textid","text_id","docid","doc_id"])
            has_yearish = any(n in names for n in ["year","date_year","decade","genre","section"])
            has_wordid  = any(n in names for n in ["wordid","word_id"])
            if has_textid and has_yearish and not has_wordid:
                csv_texts.append(lf)
        if csv_texts:
            print(f"Fallback found texts/metadata CSV shards: {len(csv_texts)}")
            lf_texts = pl.concat(csv_texts, how="vertical_relaxed")
    if texts_cands:
        print(f"Fallback found texts/metadata shards: {len(texts_cands)}")
        lf_texts = pl.concat([pl.scan_parquet(str(f)) for f in texts_cands], how="vertical_relaxed")

if lf_tokens is None:
    print("ERROR: No token shards found. Expected under 'Corpus/'."); sys.exit(1)
if lf_words is None:
    print("ERROR: No words/lexicon shards found as Parquet or CSV. Make sure the Word/Lemma/POS export is unzipped under Downloads (e.g., inside 'Corpus/' or as 'Word_lemma_POS/')."); sys.exit(1)
if lf_texts is None:
    print("WARNING: No Sources/Text shards found. Proceeding without year/genre. Download/unzip 'Sources' (or 'Text') for temporal/genre analyses.")

# ---- normalize column names ----
def norm_cols(lf, mapping):
    cols = {c.lower(): c for c in lf.collect_schema().names()}
    ren = {}
    for std, candidates in mapping.items():
        for c in candidates:
            if c in cols:
                ren[cols[c]] = std
                break
    return lf.rename(ren)

lf_tokens = norm_cols(
    lf_tokens,
    {
        "textID": ["textid","text_id","docid","doc_id"],
        "wordID": ["wordid","word_id"],
        "occID":  ["id","occurrence_id"],  # optional
    }
)

lf_words = norm_cols(
    lf_words,
    {
        "wordID": ["wordid","word_id"],
        "word":   ["word","token","form"],
        "lemma":  ["lemma"],
        "pos":    ["pos","upos","xpos","tag","PoS","poS"],
    }
)

if lf_texts is not None:
    lf_texts = norm_cols(
        lf_texts,
        {
            "textID": ["textid","text_id","docid","doc_id"],
            "year":   ["year","date_year"],
            "decade": ["decade"],
            "genre":  ["genre","section"],
        }
    )

print("TOKENS:", lf_tokens.collect_schema())
print("WORDS :", lf_words.collect_schema())
if lf_texts is not None:
    print("TEXTS :", lf_texts.collect_schema())

# ---- joins ----
# If WORDS has wordID, use tokens→words on wordID; otherwise treat WORDS as 'tagged' (per-token with textID+lemma/POS)
words_cols = set(lf_words.collect_schema().names())
texts_cols = set(lf_texts.collect_schema().names()) if lf_texts is not None else set()

def norm_texts_for_join(lf):
    # ensure textID, year, decade, genre present with consistent dtypes
    cols = set(lf.collect_schema().names())
    exprs = []
    if "textID" in cols:
        exprs.append(pl.col("textID").cast(pl.Utf8).alias("textID"))
    else:
        # try common alternates just in case
        for alt in ("textid","text_id","docid","doc_id"):
            if alt in cols:
                exprs.append(pl.col(alt).cast(pl.Utf8).alias("textID"))
                break
    if "year" in cols:
        exprs.append(pl.col("year").cast(pl.Int32).alias("year"))
    if "decade" in cols:
        exprs.append(pl.col("decade").cast(pl.Int32).alias("decade"))
    if "genre" in cols:
        exprs.append(pl.col("genre").cast(pl.Utf8).alias("genre"))
    return lf.select(exprs)

if "wordID" in words_cols:
    # Original path: tokens + lexicon on wordID
    lf_tok_words = lf_tokens.join(lf_words, on="wordID", how="left")
    # Join metadata on textID (align types)
    if lf_texts is not None:
        lf_texts_norm = norm_texts_for_join(lf_texts)
        lf_tok_meta = lf_tok_words.with_columns(pl.col("textID").cast(pl.Utf8)).join(
            lf_texts_norm,
            on="textID",
            how="left",
        )
    else:
        lf_tok_meta = lf_tok_words
else:
    # Tagged path: WORDS already has per-token word/lemma/pos keyed by textID
    # Align textID types for join with metadata
    lf_tagged = lf_words.select(
        [
            pl.col("textID").cast(pl.Utf8).alias("textID"),
            pl.col("word").cast(pl.Utf8).alias("word") if "word" in words_cols else pl.lit(None).alias("word"),
            pl.col("lemma").cast(pl.Utf8).alias("lemma") if "lemma" in words_cols else pl.lit(None).alias("lemma"),
            pl.col("pos").cast(pl.Utf8).alias("pos") if "pos" in words_cols else pl.lit(None).alias("pos"),
        ]
    )
    if lf_texts is not None:
        lf_texts_norm = norm_texts_for_join(lf_texts)
        lf_tok_meta = lf_tagged.join(lf_texts_norm, on="textID", how="left")
    else:
        lf_tok_meta = lf_tagged

# ---- products ----
OUT_DIR = BASE  # write next to the data; change if you want
names = set(lf_tok_meta.collect_schema().names())

if {"year","word"}.issubset(names):
    (
        lf_tok_meta
        .group_by(["year","word"])
        .agg(pl.len().alias("n"))
        .sink_parquet(OUT_DIR / "out_by_year_word.parquet")
    )
    print("Wrote:", OUT_DIR / "out_by_year_word.parquet")

if {"year","lemma","pos"}.issubset(names):
    (
        lf_tok_meta
        .group_by(["year","lemma","pos"])
        .agg(pl.len().alias("n"))
        .sink_parquet(OUT_DIR / "out_by_year_lemma_pos.parquet")
    )
    print("Wrote:", OUT_DIR / "out_by_year_lemma_pos.parquet")

# Top 50 lemmas per year
if {"year","lemma"}.issubset(names):
    top50 = (
        lf_tok_meta
        .group_by(["year","lemma"])
        .agg(pl.len().alias("n"))
        .sort(["year","n"], descending=[False, True])
        .group_by("year")
        .head(50)
    )
    top50_path = OUT_DIR / "out_top50_lemmas_per_year.parquet"
    top50.sink_parquet(top50_path)
    print("Wrote:", top50_path)

# quick example: print trend for a lemma
if {"year","lemma"}.issubset(names):
    TARGET = "democracy"
    demo = (
        lf_tok_meta
        .filter(pl.col("lemma") == TARGET)
        .group_by("year")
        .agg(pl.len().alias("n"))
        .sort("year")
        .collect(streaming=True)
    )
    print(demo.head(20))