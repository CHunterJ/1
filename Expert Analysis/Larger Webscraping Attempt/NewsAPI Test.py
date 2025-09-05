

#!/usr/bin/env python3
import os
import re
import csv
import requests
import pandas as pd
from transformers import pipeline
from datetime import datetime, timedelta

# ─────────────── Configuration ───────────────
# Use environment variable if set, otherwise fall back to hard-coded key
API_KEY = os.getenv("NEWSAPI_KEY", "70d976ea22714da9911aeb4c16d8d37a")

OUTPUT_CSV  = "newsapi_expert_analysis.csv"
# How far back to fetch
TO_DATE     = datetime.utcnow().date()
FROM_DATE   = TO_DATE - timedelta(days=7)
# NewsAPI endpoint
ENDPOINT    = "https://newsapi.org/v2/everything"

# ─────────────── Fetch Function ───────────────
def fetch_articles(query, from_date, to_date, pages=5, page_size=100):
    all_articles = []
    for page in range(1, pages+1):
        params = {
            "q":        query,
            "from":     from_date.isoformat(),
            "to":       to_date.isoformat(),
            "language": "en",
            "pageSize": page_size,
            "page":     page,
            "sortBy":   "publishedAt",
            "apiKey":   API_KEY
        }
        resp = requests.get(ENDPOINT, params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("articles", [])
        if not batch:
            break
        all_articles.extend(batch)
        if page * page_size >= data.get("totalResults", 0):
            break
    return all_articles

# ─────────────── Analysis Setup ───────────────
emo_pipe = pipeline(
    "text-classification",
    model="j-hartmann/emotion-english-distilroberta-base",
    return_all_scores=False
)

MODALS = {"should","must","could","would","may","might","shall","will"}
def compute_persuasion(text):
    tokens = re.findall(r"\w+", text.lower())
    if not tokens:
        return 0.0
    return sum(1 for t in tokens if t in MODALS) / len(tokens)

EXPERT_PATTERN = re.compile(
    r"\b(Dr|Prof|Professor|Mr|Ms|Mrs)\.?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*"
)

# ─────────────── Main Pipeline ───────────────
def main():
    print(f"Fetching articles from {FROM_DATE} to {TO_DATE}...")
    articles = fetch_articles(
        query="Dr OR Prof OR Professor OR Mr OR Ms",
        from_date=FROM_DATE,
        to_date=TO_DATE,
        pages=5,
        page_size=100
    )
    print(f"Retrieved {len(articles)} raw articles.")

    records = []
    for art in articles:
        source    = art.get("source", {}).get("name","")
        author    = art.get("author","") or ""
        title     = art.get("title","") or ""
        desc      = art.get("description","") or ""
        content   = art.get("content","") or ""
        url       = art.get("url","")
        published = art.get("publishedAt","")

        for field in ("title","description","content"):
            text = locals()[field]
            for match in EXPERT_PATTERN.finditer(text):
                expert  = match.group(0)
                emo     = emo_pipe(text)[0]
                pers    = round(compute_persuasion(text), 3)
                records.append({
                    "source":        source,
                    "author":        author,
                    "url":           url,
                    "publishedAt":   published,
                    "field":         field,
                    "expert":        expert,
                    "text":          text.strip(),
                    "emotion":       emo["label"],
                    "emotion_score": emo["score"],
                    "persuasion":    pers
                })

    if not records:
        print("No expert mentions found.")
        return

    fieldnames = [
        "source","author","url","publishedAt",
        "field","expert","text",
        "emotion","emotion_score","persuasion"
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Done — wrote {len(records)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()