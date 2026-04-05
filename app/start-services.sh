#!/bin/bash

set -euo pipefail

# This runs only on the master node.
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver

jps -lm
hdfs dfsadmin -report
hdfs dfsadmin -safemode leave || true

hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod -R 755 /apps/spark/jars

scala -version
jps -lm

hdfs dfs -mkdir -p /user/root
