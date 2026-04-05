#!/usr/bin/env python3

import sys


current_key = None
current_value = 0


def emit(key, value):
    record_type = key[0]
    if record_type == "DOC":
        _, doc_id, title_b64 = key
        print(f"DOC\t{doc_id}\t{title_b64}\t{value}")
    elif record_type == "CORPUS":
        _, stat_name = key
        print(f"CORPUS\t{stat_name}\t{value}")
    elif record_type == "POSTING":
        _, term, doc_id = key
        print(f"POSTING\t{term}\t{doc_id}\t{value}")


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    record_type = parts[0]

    try:
        if record_type == "DOC" and len(parts) == 4:
            key = ("DOC", parts[1], parts[2])
            value = int(parts[3])
        elif record_type == "CORPUS" and len(parts) == 3:
            key = ("CORPUS", parts[1])
            value = int(parts[2])
        elif record_type == "POSTING" and len(parts) == 4:
            key = ("POSTING", parts[1], parts[2])
            value = int(parts[3])
        else:
            continue
    except ValueError:
        continue

    if current_key == key:
        current_value += value
        continue

    if current_key is not None:
        emit(current_key, current_value)

    current_key = key
    current_value = value

if current_key is not None:
    emit(current_key, current_value)
