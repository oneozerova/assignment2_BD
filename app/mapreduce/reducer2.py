#!/usr/bin/env python3

import sys


current_term = None
current_df = 0


def emit(term, df_value):
    print(f"VOCAB\t{term}\t{df_value}")


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 3 or parts[0] != "VOCAB":
        continue

    term = parts[1]

    try:
        value = int(parts[2])
    except ValueError:
        continue

    if current_term == term:
        current_df += value
        continue

    if current_term is not None:
        emit(current_term, current_df)

    current_term = term
    current_df = value

if current_term is not None:
    emit(current_term, current_df)
