#!/usr/bin/env python3

import argparse
import math
import re
import sys

from pyspark.sql import SparkSession

from app import connect_session, ensure_schema


TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text):
    return TOKEN_RE.findall((text or "").lower())


def read_query(query_argument):
    if query_argument:
        return query_argument

    stdin_query = sys.stdin.read().strip()
    return stdin_query


def fetch_stat(session, stat_name):
    row = session.execute(
        "SELECT stat_value FROM corpus_stats WHERE stat_name = %s",
        (stat_name,),
    ).one()
    return float(row.stat_value) if row else 0.0


def bm25_score(tf_value, df_value, doc_length, total_docs, avg_doc_length, k1, b_value):
    if tf_value <= 0 or df_value <= 0 or total_docs <= 0 or avg_doc_length <= 0:
        return 0.0

    idf = math.log(total_docs / df_value)
    denominator = tf_value + k1 * ((1.0 - b_value) + b_value * (doc_length / avg_doc_length))
    return idf * (((k1 + 1.0) * tf_value) / denominator)


def parse_args():
    parser = argparse.ArgumentParser(description="Run BM25 search over the Cassandra-backed index.")
    parser.add_argument("--query", default="")
    parser.add_argument("--host", default="cassandra-server")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--k1", type=float, default=1.2)
    parser.add_argument("--b", type=float, default=0.75)
    return parser.parse_args()


def main():
    args = parse_args()
    query_text = read_query(args.query)
    query_terms = sorted(set(tokenize(query_text)))

    if not query_terms:
        print("Query is empty after tokenization.")
        return

    spark = SparkSession.builder.appName("bm25-search").getOrCreate()
    cluster, session = connect_session([args.host])

    try:
        ensure_schema(session)

        total_docs = fetch_stat(session, "doc_count")
        avg_doc_length = fetch_stat(session, "avg_doc_length")

        postings_records = []
        candidate_doc_ids = set()

        for term in query_terms:
            vocab_row = session.execute(
                "SELECT df FROM vocabulary WHERE term = %s",
                (term,),
            ).one()
            if vocab_row is None:
                continue

            df_value = int(vocab_row.df)
            posting_rows = session.execute(
                "SELECT doc_id, tf FROM postings WHERE term = %s",
                (term,),
            )

            for posting in posting_rows:
                postings_records.append((term, posting.doc_id, int(posting.tf), df_value))
                candidate_doc_ids.add(posting.doc_id)

        if not postings_records:
            print("No matching documents were found.")
            return

        documents = {}
        for doc_id in candidate_doc_ids:
            row = session.execute(
                "SELECT title, doc_length FROM documents WHERE doc_id = %s",
                (doc_id,),
            ).one()
            if row is not None:
                documents[doc_id] = {"title": row.title, "doc_length": int(row.doc_length)}

        doc_broadcast = spark.sparkContext.broadcast(documents)
        total_docs_broadcast = spark.sparkContext.broadcast(total_docs)
        avg_doc_length_broadcast = spark.sparkContext.broadcast(avg_doc_length)

        scores = (
            spark.sparkContext.parallelize(postings_records, max(1, min(8, len(postings_records))))
            .map(
                lambda record: (
                    record[1],
                    bm25_score(
                        record[2],
                        record[3],
                        doc_broadcast.value[record[1]]["doc_length"],
                        total_docs_broadcast.value,
                        avg_doc_length_broadcast.value,
                        args.k1,
                        args.b,
                    ),
                )
            )
            .reduceByKey(lambda left, right: left + right)
            .takeOrdered(args.limit, key=lambda item: -item[1])
        )

        for rank, (doc_id, score) in enumerate(scores, start=1):
            title = documents[doc_id]["title"]
            print(f"{rank}\t{doc_id}\t{title}\t{score:.6f}")
    finally:
        session.shutdown()
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
