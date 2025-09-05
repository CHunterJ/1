import requests
from bs4 import BeautifulSoup
import csv

url = "https://www.bbc.com/news/articles/clylv796ekgo"
headers = {"User-Agent": "Mozilla/5.0 (compatible)"}
response = requests.get(url, headers=headers)
response.raise_for_status()
html = response.text

soup = BeautifulSoup(html, "lxml")

# Extract the main headline
headline = soup.find("h1")
if headline:
    print("Headline:", headline.get_text(strip=True))
else:
    print("Headline not found")

# Extract article paragraphs into a list of dicts
article = soup.find("article")
rows = []
if article:
    for p in article.find_all("p"):
        text = p.get_text(strip=True)
        rows.append({"paragraph": text})
else:
    print("Article body not found")

# Sort paragraphs alphabetically
rows_sorted = sorted(rows, key=lambda r: r["paragraph"])

# Write sorted paragraphs to CSV
with open("paragraphs.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["paragraph"])
    writer.writeheader()
    writer.writerows(rows_sorted)
print(f"Wrote {len(rows_sorted)} paragraphs to paragraphs.csv")