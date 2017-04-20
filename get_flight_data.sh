#!/bin/bash
for i in {1987..2008}
do
   wget "http://stat-computing.org/dataexpo/2009/$i.csv.bz2"
   bzip2 -d "$i.csv.bz2"
   tail -n +2 "$i.csv" > "$i.csv.tmp" && mv "$i.csv.tmp" "$i.csv"
done