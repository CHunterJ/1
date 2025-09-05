#!/usr/bin/env python3
import pandas as pd

# 1. Load both files
ls   = pd.read_csv("articles (LS output 2022).csv")
templ = pd.read_csv("opinion_articles_2000.csv")

# 2. Build the matching 'filename' key on the template side
#    (assuming templ.id is 1,2,3 ... matching article_001.txt etc)
templ["filename"] = (
    templ["id"]
      .astype(str)
      .str.zfill(len(str(templ["id"].max())))
      .radd("article_")
      .add(".txt")
)

# 3. Strip whitespace on both sides
templ["filename"] = templ["filename"].str.strip()
ls["filename"]    = ls["filename"].str.strip()

# 4. Merge
df = templ.merge(ls, on="filename", how="left", validate="one_to_one")

# 5. Fill missing numeric columns with 0 (this removes those “empty spaces”)
num_cols = df.select_dtypes(include="number").columns
df[num_cols] = df[num_cols].fillna(0)

# 6. Reorder: first all template cols, then your LS‐generated cols
templ_cols = list(templ.columns)
ls_cols     = [c for c in ls.columns if c not in ("filename",)]
df = df[templ_cols + ls_cols]

# 7. Save out
out = "opinion_articles_2022_clean.csv"
df.to_csv(out, index=False)
print(f"✅ Wrote cleaned file → {out}")
