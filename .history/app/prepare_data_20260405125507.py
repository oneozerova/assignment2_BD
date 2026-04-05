import argparse
import os
import re
import unicodedata
from pathlib import Path
from urllib.parse import urlparse

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


WHITESPACE_RE = re.compile(r"\s+")


def normalize_whitespace(text):
    return WHITESPACE_RE.sub(" ", text or "").strip()


def resolve_output_dir(path_text):
    output_dir = Path(path_text).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def resolve_parquet_path(path_text):
    parsed = urlparse(path_text)
    if parsed.scheme:
        return path_text
    return Path(path_text).resolve().as_uri()


def clean_txt_files(output_dir):
    for file_path in Path(output_dir).glob("*.txt"):
        file_path.unlink()


def build_filename(doc_id, title):
    safe_title = sanitize_filename(str(title or "untitled")).replace(" ", "_")
    ascii_title = (
        unicodedata.normalize("NFKD", safe_title)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    ascii_title = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_title).strip("._")
    if not ascii_title:
        ascii_title = "untitled"
    return f"{doc_id}_{ascii_title}.txt"


def generate_docs(spark, parquet_path, output_dir, count, seed):
    parquet_uri = resolve_parquet_path(parquet_path)
    df = (
        spark.read.parquet(parquet_uri)
        .select("id", "title", "text")
        .where(F.col("id").isNotNull())
        .where(F.col("title").isNotNull())
        .where(F.col("text").isNotNull())
        .where(F.length(F.trim(F.col("text"))) > 0)
        .orderBy(F.rand(seed))
        .limit(count)
    )

    output_dir = resolve_output_dir(output_dir)
    clean_txt_files(output_dir)

    def create_doc(row):
        file_name = build_filename(row["id"], row["title"])
        file_path = output_dir / file_name
        with open(file_path, "w", encoding="utf-8") as file_handle:
            file_handle.write(row["text"])

    df.foreach(create_doc)


def parse_doc_metadata(file_path):
    filename = Path(file_path).stem
    if "_" not in filename:
        return filename, filename

    doc_id, raw_title = filename.split("_", 1)
    title = raw_title.replace("_", " ")
    return doc_id, title


def build_input_from_hdfs(spark, source_path, output_path):
    sc = spark.sparkContext
    input_pattern = source_path.rstrip("/") + "/*.txt"

    def format_record(entry):
        file_path, content = entry
        doc_id, title = parse_doc_metadata(file_path)
        text = normalize_whitespace(content)
        if not text:
            return None
        return "\t".join([doc_id, title, text])

    (
        sc.wholeTextFiles(input_pattern)
        .map(format_record)
        .filter(lambda line: line is not None)
        .coalesce(1)
        .saveAsTextFile(output_path)
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Prepare data for the search engine assignment.")
    parser.add_argument(
        "--mode",
        choices=["generate-docs", "build-input"],
        required=True,
        help="generate-docs creates local txt files from parquet; build-input reads txt files from HDFS and writes /input/data.",
    )
    parser.add_argument("--parquet", default="data/n.parquet")
    parser.add_argument("--output-dir", default="data/generated_docs")
    parser.add_argument("--source-path", default="/data")
    parser.add_argument("--output-path", default="/input/data")
    parser.add_argument("--count", type=int, default=int(os.environ.get("DOC_COUNT", "100")))
    parser.add_argument("--seed", type=int, default=0)
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("data preparation")
        .master("local[*]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

    if args.mode == "generate-docs":
        generate_docs(spark, args.parquet, args.output_dir, args.count, args.seed)
    else:
        build_input_from_hdfs(spark, args.source_path, args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()
