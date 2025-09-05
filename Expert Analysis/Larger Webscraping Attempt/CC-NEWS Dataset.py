#!/usr/bin/env python3
import gzip
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import boto3
from botocore import UNSIGNED
from botocore.client import Config

# 0. Configure unsigned S3 client for CC-NEWS (Requester Pays)
client = boto3.client(
    "s3",
    config=Config(signature_version=UNSIGNED),
    region_name="us-east-1"
)
paginator = client.get_paginator("list_objects_v2")

# 1. List WARC files directly under cc-news/
warcs = []
for page in paginator.paginate(Bucket="commoncrawl", Prefix="cc-news/", RequestPayer="requester"):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".warc.gz"):
            warcs.append(key)
    # Stop once we have at least 5 samples
    if len(warcs) >= 5:
        break
print(f"Found {len(warcs)} WARC files; showing first 5:")
for key in warcs[:5]:
    print(" ", key)

# 2. Download one sample WARC via S3 get_object
sample_key = warcs[0]
print(f"\nDownloading sample {sample_key} …")
resp = client.get_object(Bucket="commoncrawl", Key=sample_key, RequestPayer="requester")
with open("sample.warc.gz", "wb") as f:
    f.write(resp["Body"].read())
print("Done. Extracting text…")

# 3. Extract article text from sample.warc.gz
with gzip.open("sample.warc.gz", "rb") as stream, open("sample.txt", "w", encoding="utf-8") as out:
    for record in ArchiveIterator(stream):
        if record.rec_type == "response":
            ctype = record.http_headers.get_header("Content-Type") or ""
            if "text/html" in ctype:
                html = record.content_stream().read().decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html, "lxml")
                art = soup.find("article")
                if art:
                    text = art.get_text(separator=" ", strip=True)
                    out.write(text + "\n\n")

print("Sample extraction written to sample.txt")