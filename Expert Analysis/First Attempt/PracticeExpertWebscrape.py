import csv
from transformers import pipeline
import requests
from bs4 import BeautifulSoup
import re

# Simple persuasion heuristic: modal-verb density
MODALS = {"should", "must", "could", "would", "may", "might", "shall", "will"}
def compute_persuasion(text):
    words = re.findall(r"\w+", text.lower())
    return sum(1 for w in words if w in MODALS) / len(words) if words else 0.0

# 0. Configuration for scraping
URL = "https://www.bbc.com/news/articles/clylv796ekgo"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}

# 1. Scrape & extract into `data`
resp = requests.get(URL, headers=HEADERS)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "lxml")

data = []
# Pattern to find expert titles and names
pattern = re.compile(r"\b(?:Dr|Prof|Professor|Mr|Ms|Mrs)\.?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*")

# 1A. Extract from blockquotes
for block in soup.find_all("blockquote"):
    text = block.get_text(strip=True)
    experts = pattern.findall(text)
    for expert in experts:
        data.append({"expert": expert, "quote": text})

# 1B. Extract from paragraphs (excluding those inside blockquotes)
for p in soup.find_all("p"):
    if p.find_parent("blockquote"):
        continue
    text = p.get_text(strip=True)
    experts = pattern.findall(text)
    for expert in experts:
        data.append({"expert": expert, "quote": text})

# 2. Load pipelines
emo_pipe = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base")

results = []
for row in data:
    emotions = emo_pipe(row["quote"])[0]             # e.g. {"label":"joy", "score":0.87}
    persuasion_score = compute_persuasion(row["quote"])
    results.append({
        "expert": row["expert"],
        "quote": row["quote"],
        "emotion_label": emotions["label"],
        "emotion_score": emotions["score"],
        "persuasion_score": round(persuasion_score, 3)
    })

# 3. Save to CSV
fieldnames = ["expert", "quote", "emotion_label", "emotion_score", "persuasion_score"]
if not results:
    print("No expert quotes found; no CSV generated.")
    exit()

with open("PracticeExpertAnalysis.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)
    print(f"Wrote {len(results)} rows to PracticeExpertAnalysis.csv")