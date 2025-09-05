#!/usr/bin/env python3
import pandas as pd
import os

# 1. Filenames / paths – adjust if yours are different
metadata_csv = "Metadata NYT 2022.csv"
txt_folder    = "articles"               # or whatever folder holds your 2022 .txt files
output_csv    = "opinion_articles_2022_combined.csv"

# 2. Load metadata
df = pd.read_csv(metadata_csv)

# 3. Ensure we have a 'filename' column matching article_###.txt
if "filename" not in df.columns:
    if "id" in df.columns:
        width = len(str(df["id"].max()))
        # Build filenames like "article_001.txt"
        df["filename"] = (
            "article_"
            + df["id"].astype(str).str.zfill(width)
            + ".txt"
        )
    else:
        raise KeyError("Metadata CSV must have either a 'filename' or an 'id' column")

# 4. Read each text file into a new 'text' column
texts = []
for fname in df["filename"]:
    path = os.path.join(txt_folder, fname)
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            texts.append(f.read().strip())
    else:
        texts.append("")  # leaves blank if the .txt is missing

df["text"] = texts

# 5. Save the combined CSV
df.to_csv(output_csv, index=False)
print(f"✅ Wrote combined file → {output_csv}")