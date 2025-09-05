import polars as pl

# Inspect the lemma cube
df = pl.read_parquet("/Users/christopherjorgensen/Downloads/out_by_year_lemma_pos.parquet")
print(df.head(10))
print(df.filter((pl.col("lemma")=="democracy")).sort("year").head(10))

# Top 50 lemmas per year
top = pl.read_parquet("/Users/christopherjorgensen/Downloads/out_top50_lemmas_per_year.parquet")
print(top.filter(pl.col("year")==1900))