#!/usr/bin/env python3

import sys


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4 or parts[0] != "POSTING":
        continue

    _, term, _, _ = parts
    print(f"VOCAB\t{term}\t1")
