#!/bin/bash
for i in {1987..2008}
do
    curl http://stat-computing.org/dataexpo/2009/$i.csv.bz2 | bzip2 -d | hdfs dfs -copyFromLocal - /data/$i.csv
done