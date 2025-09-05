#!/usr/bin/env python3
import pandas as pd
import sys

# 1️⃣ Load both CSVs
ls = pd.read_csv("articles (LS output 2022).csv")
template = pd.read_csv("opinion_articles_2000.csv")

# 2️⃣ Identify the join‐key
#    In your LS export it's 'filename'; in the template there isn't a 'filename' column,
#    so we’ll construct one from the template’s 'id' (assuming id == document number).
#    Adjust this bit if your template uses a different key!
template["filename"] = template["id"].astype(str).str.zfill(3).apply(
    lambda x: f"article_{x}.txt"
)

# 3️⃣ Merge the two DataFrames
#    Left‐merge the template (metadata + text) with the LS measures.
df = pd.merge(
    template,
    ls,
    on="filename",
    how="left",
    validate="one_to_one"
)

# 4️⃣ Reorder columns to exactly match the template file,
#    then append all LS‐generated columns at the end.
template_cols = list(template.columns)  # this includes your metadata + combined_text
ls_only = [c for c in ls.columns if c not in ("filename",)]
out_cols = template_cols + ls_only

df = df[out_cols]

# 5️⃣ Write out the new CSV
out_name = "Metadata NYT 2022.csv"
df.to_csv(out_name, index=False)
print(f"Wrote reformatted file → {out_name}")