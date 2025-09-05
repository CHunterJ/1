#!/usr/bin/env python3
import pandas as pd

# ─────────────── Load your data ───────────────
CSV_PATH = "expert_analysis_all_news.csv"  # adjust if needed
df = pd.read_csv(CSV_PATH)

# ─────────────── Basic Structure ───────────────
print("=== DataFrame Info ===")
df.info()
print("\n=== First 5 Rows ===")
print(df.head(), "\n")

# ─────────────── Missingness Check ───────────────
print("=== Missing Values per Column ===")
print(df.isna().sum(), "\n")

# ─────────────── Numeric Summaries ───────────────
print("=== Persuasion Score Summary ===")
print(df["persuasion_score"].describe(), "\n")

print("=== Emotion Score Summary ===")
print(df["emotion_score"].describe(), "\n")

# ─────────────── Categorical Distributions ───────────────
print("=== Top 10 Experts by Frequency ===")
print(df["expert"].value_counts().head(10), "\n")

print("=== Emotion Label Distribution ===")
print(df["emotion_label"].value_counts(), "\n")

# ─────────────── Optional: Pivot Table ───────────────
if "certainty_flag" in df.columns:
    print("=== Mean Persuasion by Certainty ===")
    pivot = df.pivot_table(
        index="certainty_flag",
        values="persuasion_score",
        aggfunc=["mean", "count"]
    )
    print(pivot, "\n")

print("=== Done ===")