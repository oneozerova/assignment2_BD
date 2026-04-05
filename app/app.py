#!/usr/bin/env python3

import argparse
import base64
import socket
import subprocess
import time
from glob import glob

from cassandra.cluster import Cluster


KEYSPACE = "search_engine"


def decode_title(encoded_title):
    return base64.urlsafe_b64decode(encoded_title.encode("ascii")).decode("utf-8")


def connect_session(hosts=None, retries=30, delay_seconds=5):
    raw_hosts = hosts or ["cassandra-server", "cassandra"]
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            resolved_hosts = []
            for host in raw_hosts:
                try:
                    socket.getaddrinfo(host, 9042)
                    resolved_hosts.append(host)
                except socket.gaierror:
                    continue

            candidate_hosts = resolved_hosts or raw_hosts
            cluster = Cluster(candidate_hosts)
            session = cluster.connect()
            return cluster, session
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            try:
                cluster.shutdown()
            except Exception:  # noqa: BLE001
                pass
            if attempt == retries:
                break
            time.sleep(delay_seconds)

    raise RuntimeError(f"Unable to connect to Cassandra after {retries} attempts: {last_error}")


def ensure_schema(session):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_name text PRIMARY KEY,
            stat_value double
        )
        """
    )


def truncate_tables(session):
    for table_name in ["postings", "vocabulary", "documents", "corpus_stats"]:
        session.execute(f"TRUNCATE {table_name}")


def hdfs_cat_lines(hdfs_path):
    command = ["hdfs", "dfs", "-cat", hdfs_path.rstrip("/") + "/part-*"]
    output = subprocess.check_output(command, text=True)
    return [line for line in output.splitlines() if line.strip()]


def wait_for_hdfs_output(hdfs_path, retries=12, delay_seconds=5):
    pattern = hdfs_path.rstrip("/") + "/part-*"
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            subprocess.check_call(
                ["hdfs", "dfs", "-ls", pattern],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(delay_seconds)

    raise RuntimeError(f"HDFS output files were not found for {hdfs_path}: {last_error}")


def load_index(session, pipeline1_path, pipeline2_path):
    wait_for_hdfs_output(pipeline1_path)
    wait_for_hdfs_output(pipeline2_path)

    truncate_tables(session)

    insert_document = session.prepare(
        "INSERT INTO documents (doc_id, title, doc_length) VALUES (?, ?, ?)"
    )
    insert_posting = session.prepare(
        "INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)"
    )
    insert_vocab = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    insert_corpus_stat = session.prepare(
        "INSERT INTO corpus_stats (stat_name, stat_value) VALUES (?, ?)"
    )

    corpus_totals = {"DOC_COUNT": 0.0, "TOTAL_DOC_LENGTH": 0.0}

    for line in hdfs_cat_lines(pipeline1_path):
        parts = line.split("\t")
        record_type = parts[0]

        if record_type == "DOC" and len(parts) == 4:
            _, doc_id, title_b64, doc_length = parts
            session.execute(insert_document, (doc_id, decode_title(title_b64), int(doc_length)))
        elif record_type == "POSTING" and len(parts) == 4:
            _, term, doc_id, tf_value = parts
            session.execute(insert_posting, (term, doc_id, int(tf_value)))
        elif record_type == "CORPUS" and len(parts) == 3:
            _, stat_name, stat_value = parts
            corpus_totals[stat_name] = corpus_totals.get(stat_name, 0.0) + float(stat_value)

    for line in hdfs_cat_lines(pipeline2_path):
        parts = line.split("\t")
        if len(parts) != 3 or parts[0] != "VOCAB":
            continue
        _, term, df_value = parts
        session.execute(insert_vocab, (term, int(df_value)))

    doc_count = corpus_totals.get("DOC_COUNT", 0.0)
    total_doc_length = corpus_totals.get("TOTAL_DOC_LENGTH", 0.0)
    avg_doc_length = total_doc_length / doc_count if doc_count else 0.0

    session.execute(insert_corpus_stat, ("doc_count", float(doc_count)))
    session.execute(insert_corpus_stat, ("total_doc_length", float(total_doc_length)))
    session.execute(insert_corpus_stat, ("avg_doc_length", float(avg_doc_length)))


def parse_args():
    parser = argparse.ArgumentParser(description="Utilities for loading the search index into Cassandra.")
    parser.add_argument("--load-index", action="store_true")
    parser.add_argument("--pipeline1-path", default="/indexer/pipeline1")
    parser.add_argument("--pipeline2-path", default="/indexer/pipeline2")
    parser.add_argument("--host", default="cassandra-server,cassandra")
    return parser.parse_args()


def main():
    args = parse_args()
    cluster, session = connect_session([host.strip() for host in args.host.split(",") if host.strip()])

    try:
        ensure_schema(session)
        if args.load_index:
            load_index(session, args.pipeline1_path, args.pipeline2_path)
            print("Index loaded into Cassandra.")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
