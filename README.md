For Local Development :
Download Spark 1.5.1 for Hadoop 2.6 (http://spark.apache.org/downloads.html)
Add Jars to IDE

###For Development in CS Lab :
cs computers - spark-1.5.1-bin-hadoop2.6

###Flight Data :
http://stat-computing.org/dataexpo/2009/the-data.html

Stage Data in Local Hadoop Cluster ($HADOOP_HOME/bin/hdfs dfs -put <path_to_data>)

###Run Wordcount
Example:
/usr/local/spark-1.5.1-bin-hadoop2.6/bin/spark-submit --jars flight_analysis.jar --class cs455.flightdata.spark.WordCount flight_analysis.jar hdfs://salem.cs.colostate.edu:50000/data/flight_data

Note:
WordCount works with books taken from Assignment 3 instructions
Running wordCount on the Flight Data hasn't been successful yet; currently looking into it
This may have to do with the dataset

### Run Flight Data Analysis
I'm starting to look at the charting, so wanted some realistic data to work with.  I ran Ben's initial exmple:

``` nohup spark-submit --master yarn --class cs455.flightdata.spark.FlightDataAnalysis flight_analysis.jar /data/flightdata 2>error.txt 1> flightdataanalysis.txt &```

Notes:

I had added /usr/local/spark-1.5.1-bin-hadoop2.6/bin/ to my path.

`nohup` ignores the terminal closing, so will continue running (with the &) when my computer sleeps.

I redirect stderr to `error.txt` (lots of spark output), and the actual output to `flightdataanalysis.txt`.  

My data is stored in my hdfs cluster in /data/flightdata