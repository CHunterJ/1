#!/usr/bin/env python3
import pandas as pd
import re
import csv
from transformers import pipeline

# ──────────────── Config ─────────────────
import os
# Locate the input CSV file in known locations
_CANDIDATE_PATHS = [
    os.path.expanduser("~/Downloads/all-the-news-2-1.csv"),
    "/mnt/data/all-the-news-2-1.csv",
    os.path.join(os.path.dirname(__file__), "all-the-news-2-1.csv"),
    "all-the-news-2-1.csv"
]
for path in _CANDIDATE_PATHS:
    if os.path.exists(path):
        INPUT_CSV = path
        break
else:
    print(f"Error: input file not found. Checked: {_CANDIDATE_PATHS}")
    exit(1)
OUTPUT_CSV = "expert_analysis_all_news.csv"
# Initialize Hugging Face emotion pipeline
emo_pipe = pipeline(
    "text-classification",
    model="j-hartmann/emotion-english-distilroberta-base",
    return_all_scores=False
)
# Persuasion heuristic: modal‐verb density
MODALS = {"should", "must", "could", "would", "may", "might", "shall", "will"}


def compute_persuasion(text):
    words = re.findall(r"\w+", text.lower())
    return sum(1 for w in words if w in MODALS) / len(words) if words else 0.0


# Pattern to identify expert titles & names (e.g. “Dr Jane Smith”)
EXPERT_PATTERN = re.compile(
    r"\b(?:Dr|Prof|Professor|Mr|Ms|Mrs)\.?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*"
)
# Sentence splitter
SENT_SPLIT = re.compile(r"(?<=[\.!?])\s+")


# ──────────────── Main ──────────────────
def extract_from_text(text, source_id):
    """
    Return a list of dicts for each expert‐sentence pair in `text`.
    source_id can be the URL, filename, or DataFrame index.
    """
    results = []
    for sentence in SENT_SPLIT.split(text):
        experts = EXPERT_PATTERN.findall(sentence)
        if not experts:
            continue
        # classify emotion once per sentence
        emo = emo_pipe(sentence)[0]
        pers = round(compute_persuasion(sentence), 3)
        for expert in experts:
            results.append({
                "source": source_id,
                "expert": expert,
                "quote": sentence.strip(),
                "emotion_label": emo["label"],
                "emotion_score": emo["score"],
                "persuasion_score": pers
            })
    return results


def main():
    # 1. Load the All-The-News dataset
    print(f"Loading articles from {INPUT_CSV}…")
    df = pd.read_csv(INPUT_CSV, usecols=["content"], dtype=str)

    all_entries = []
    for idx, row in df.iterrows():
        content = row["content"] or ""
        # you could also pass row["url"] or row["id"] as source,
        # but All-The-News might not include URLs
        entries = extract_from_text(content, source_id=idx)
        all_entries.extend(entries)
        if idx % 1000 == 0 and idx > 0:
            print(f"  Processed {idx} articles, found {len(all_entries)} expert quotes so far…")

    if not all_entries:
        print("No expert quotes found in any article.")
        return

    # 2. Write to CSV
    fieldnames = ["source", "expert", "quote", "emotion_label", "emotion_score", "persuasion_score"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_entries)
    print(f"Done—wrote {len(all_entries)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()