import os

# 1. Point to your “no‐TOC” file
IN_FILE = "no_toc_articles.txt"  # ← replace with your filename
OUT_DIR = "Repplication/articles"
DELIMITER = "Document "          # ← or whatever your file uses

# 2. Read the entire file
with open(IN_FILE, "r", encoding="utf-8") as f:
    blob = f.read()

# 3. Split on the delimiter (drops everything before the first occurrence)
chunks = blob.split(DELIMITER)[1:]

os.makedirs(OUT_DIR, exist_ok=True)

# 4. Write each chunk back out as its own .txt
for i, chunk in enumerate(chunks, 1):
    text = DELIMITER + chunk.strip()
    fn = f"article_{i:03d}.txt"
    with open(os.path.join(OUT_DIR, fn), "w", encoding="utf-8") as out:
        out.write(text)
    print(f"Wrote {fn}")