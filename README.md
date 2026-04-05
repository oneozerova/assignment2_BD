# big-data-assignment2

Simple search engine pipeline built with Docker, Hadoop, Spark, and Cassandra.

## Prerequisites

- Docker
- Docker Compose

## Repository layout

- `app/`: application code, shell scripts, and Python jobs
- `app/data/`: parquet input file and generated `.txt` documents
- `docker-compose.yml`: local cluster definition

## Input data

Place the parquet file in one of these locations before starting the stack:

- `app/data/n.parquet`
- `app/n.parquet`
- `app/data/a.parquet`
- `app/a.parquet`

The default configuration prepares `100` documents. You can override this with `DOC_COUNT`.

## Start the stack

```bash
docker compose up
```

This starts:

- `cluster-master`: Hadoop master, Spark client environment, application scripts
- `cluster-slave-1`: worker container
- `cassandra-server`: Cassandra database

On startup, the master container:

1. starts HDFS, YARN, and the MapReduce history server
2. creates `.venv` if needed
3. installs Python dependencies from `app/requirements.txt` when the requirements hash changes

By default, it does not run the full indexing workflow automatically. The container stays up and prints the commands to run manually.

## Default workflow

Open a shell in the master container:

```bash
docker compose exec cluster-master bash
```

Then run the pipeline in this order:

```bash
bash /app/prepare_data.sh
bash /app/index.sh /input/data
bash /app/search.sh "history of science"
```

What each command does:

- `prepare_data.sh`: reads the parquet file, generates local `.txt` documents, uploads them to HDFS `/data`, and builds a single-partition dataset in HDFS `/input/data`
- `index.sh /input/data`: builds the inverted index with Hadoop Streaming and stores it in Cassandra
- `search.sh "..."`: runs a BM25 search; it tries YARN first and falls back to local Spark if needed

## Automatic workflow

If you want the full workflow to run during container startup, set `AUTO_RUN_WORKFLOW=1` for `cluster-master` in `docker-compose.yml`.

Example:

```yaml
services:
  cluster-master:
    environment:
      AUTO_RUN_WORKFLOW: "1"
      RUN_SAMPLE_QUERIES: "1"
```

Notes:

- `AUTO_RUN_WORKFLOW=1` runs `prepare_data.sh` and `index.sh`
- `RUN_SAMPLE_QUERIES=1` additionally runs sample searches after indexing

## Useful environment variables

- `DOC_COUNT`: number of documents to sample from the parquet file, default `100`
- `AUTO_RUN_WORKFLOW`: `0` by default
- `RUN_SAMPLE_QUERIES`: `0` by default
- `SEARCH_TIMEOUT_SEC`: timeout for YARN search before falling back to local Spark, default `180`
- `LOCAL_DOCS_DIR`: local output folder for generated documents, default `data/generated_docs`
- `HDFS_DOCS_DIR`: HDFS folder for raw documents, default `/data`
- `HDFS_INPUT_DIR`: HDFS folder for prepared input rows, default `/input/data`

## Common commands

Start containers in the background:

```bash
docker compose up -d
```

Follow master logs:

```bash
docker compose logs -f cluster-master
```

Stop the stack:

```bash
docker compose down
```
