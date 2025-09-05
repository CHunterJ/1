#!/usr/bin/env python3
import os
import glob
import re
import sys

def find_input_file():
    # 1) If the user passed a filename, use that
    if len(sys.argv) > 1:
        return sys.argv[1]
    # 2) Otherwise auto‐detect any ProQuestDocuments-*.txt
    candidates = glob.glob("ProQuestDocuments-*.txt")
    if candidates:
        return candidates[0]
    # 3) Fallback: any lone .txt in the cwd
    candidates = glob.glob("*.txt")
    if len(candidates) == 1:
        return candidates[0]
    # 4) Otherwise error out
    print("❌ Could not find a single .txt to process. "
          "Either pass the filename as `python split_proquest.py <file.txt>` or "
          "place exactly one .txt in this folder.")
    sys.exit(1)

def main():
    infile = find_input_file()
    print(f"ℹ️  Input file: {infile}")

    with open(infile, "r", encoding="utf-8") as f:
        content = f.read()

    # Match “Document 1 of 566” markers (case-insensitive)
    marker_re = re.compile(r"(Document\s+(\d+)\s+of\s+(\d+))", re.IGNORECASE)
    matches = list(marker_re.finditer(content))
    if not matches:
        print("❌ No “Document X of Y” markers found in the file.")
        sys.exit(1)

    total = int(matches[0].group(3))
    width = len(str(total))

    out_dir = "Repplication/articles"
    os.makedirs(out_dir, exist_ok=True)

    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i+1].start() if i+1 < len(matches) else len(content)
        chunk = content[start:end].strip()

        num = int(m.group(2))
        fname = f"article_{num:0{width}d}.txt"
        path = os.path.join(out_dir, fname)

        with open(path, "w", encoding="utf-8") as out:
            out.write(chunk)
        print(f"✅ Wrote {path}")

if __name__ == "__main__":
    main()