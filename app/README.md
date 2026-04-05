## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the parquet file and generated plain text documents. The current implementation expects `n.parquet` and prepares `n=100` documents into `data/generated_docs`.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

### app.py
This Python file creates the Cassandra schema and loads index data from HDFS into Cassandra.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to run two Hadoop Streaming pipelines and store their outputs in HDFS under `/indexer`.

### index.sh
A script to run `create_index.sh` and then `store_index.sh`.

### prepare_data.py
The PySpark script used both for generating `.txt` documents from parquet and for building `/input/data` in HDFS from `/data`.

### prepare_data.sh
The script that will run the prevoious Python file and will copy the data to HDFS.

### query.py
A PySpark BM25 ranker that reads the index from Cassandra and retrieves the top 10 relevant documents.

### requirements.txt
This file contains all Python depenedencies that are needed for running the programs in this repository. This file is read by pip when installing the dependencies in `app.sh` script.

### search.sh
This script runs the BM25 query job on the YARN cluster with the packaged Python environment.


### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.


### store_index.sh
This script creates Cassandra tables and loads the index data from HDFS to them.
