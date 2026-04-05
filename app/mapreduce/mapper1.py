#!/usr/bin/env python3

import base64
import re
import sys
from collections import Counter


TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text):
    return TOKEN_RE.findall(text.lower())


def encode_title(title):
    return base64.urlsafe_b64encode(title.encode("utf-8")).decode("ascii")


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    tokens = tokenize(text)
    if not tokens:
        continue

    title_b64 = encode_title(title)
    doc_length = len(tokens)
    term_counts = Counter(tokens)

    print(f"DOC\t{doc_id}\t{title_b64}\t{doc_length}")
    print("CORPUS\tDOC_COUNT\t1")
    print(f"CORPUS\tTOTAL_DOC_LENGTH\t{doc_length}")

    for term, tf_value in sorted(term_counts.items()):
        print(f"POSTING\t{term}\t{doc_id}\t{tf_value}")
