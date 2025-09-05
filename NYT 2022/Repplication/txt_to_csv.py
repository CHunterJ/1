#!/usr/bin/env python3
import re
import csv

INPUT_TXT = "Full Text Articles NYT 2022.txt"
OUTPUT_CSV = "articles_2022.csv"

# 1. Read the entire dump
with open(INPUT_TXT, "r", encoding="utf-8") as f:
    blob = f.read()

# 2. Split on the Document markers, but keep the header line
parts = re.split(r'(?m)^(Document\s+\d+\s+of\s+\d+)\s*$', blob)
# re.split gives: [pre, "Document 1 of Y", content1, "Document 2 of Y", content2, ...]
records = []
for i in range(1, len(parts), 2):
    header = parts[i]      # e.g. "Document 1 of 566"
    body   = parts[i+1]    # the text from after that header up to the next header

    rec = {"doc_marker": header}
    lines = body.splitlines()

    # 3. Extract the early‐on fields by regex
    def grab(pat):
        for L in lines:
            m = re.match(pat, L)
            if m:
                return m.group(1).strip()
        return ""

    rec["title"]             = grab(r'^(.*?)\s*:\s*\[Op-Ed\]')
    rec["author"]            = grab(r'^Author:\s*(.*)')
    rec["publication_info"]  = grab(r'^Publication info:\s*(.*)')
    rec["url"]               = grab(r'^(https?://\S+)')
    rec["abstract"]          = grab(r'^Abstract:\s*(.*)')
    rec["links"]             = grab(r'^Links:\s*(\S.*)')
    rec["publication_date"]  = grab(r'^Publication date:\s*(.*)')
    rec["publication_title"] = grab(r'^Publication title:\s*(.*)')
    rec["section"]           = grab(r'^Section:\s*(.*)')
    rec["issn"]              = grab(r'^ISSN:\s*(.*)')
    rec["doc_type"]          = grab(r'^Document type:\s*(.*)')
    rec["proquest_id"]       = grab(r'^ProQuest document ID:\s*(.*)')

    # 4. Pull out the full‐text block
    #    Find the line “Full text:” then collect until the first bottom‐metadata marker (e.g. "Subject:")
    fulltext = []
    in_text = False
    for L in lines:
        if in_text:
            if re.match(r'^(Subject|Business indexing term|Location|Company / organization):', L):
                break
            fulltext.append(L)
        elif L.startswith("Full text:"):
            in_text = True
            fulltext.append(L.replace("Full text:", "").strip())

    rec["full_text"] = "\n".join(fulltext).strip()
    records.append(rec)

# 5. Write to CSV
fieldnames = [
    "doc_marker","title","author","publication_info","url","abstract","links",
    "publication_date","publication_title","section","issn","doc_type","proquest_id",
    "full_text"
]

with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as outf:
    writer = csv.DictWriter(outf, fieldnames=fieldnames)
    writer.writeheader()
    for r in records:
        writer.writerow(r)

print(f"Wrote {len(records)} articles → {OUTPUT_CSV}")
