#!/usr/bin/env python3
import pandas as pd

# 1) Load your LS export
ls = pd.read_csv("articles (LS output 2022).csv")

# 2) (Optional) If you have a separate 2022‐metadata file, load that here:
# meta = pd.read_csv("opinion_articles_2022_metadata.csv")
# meta['filename'] = (
#     meta['id']
#       .astype(str)
#       .str.zfill(len(str(meta['id'].max())))
#       .radd('article_')
#       .add('.txt')
# )
# df = meta.merge(ls, on='filename', how='left', validate='one_to_one')

# But if you want to just use the LS export’s own columns:
df = ls.copy()

# 3) If you need an 'id' column parsed out of the filename:
df['id'] = (
    df['filename']
      .str.extract(r'(\d+)')       # grab the number
      .astype(int)
)

# 4) Reorder columns however you like. For example:
#    id, filename, then all the LS measures
cols = ['id', 'filename'] + [c for c in df.columns if c not in ('id', 'filename')]
df = df[cols]

# 5) Write your final CSV, keeping blanks intact
df.to_csv("opinion_articles_2022_reformatted.csv",
          index=False,
          na_rep='')
print("✅ Wrote opinion_articles_2022_reformatted.csv")
