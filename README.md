For Local Development :
Download Spark 1.5.1 for Hadoop 2.6 (http://spark.apache.org/downloads.html)
Add Jars to IDE

For Development in CS Lab :
cs computers - spark-1.5.1-bin-hadoop2.6

Flight Data :
http://stat-computing.org/dataexpo/2009/the-data.html

Stage Data in Local Hadoop Cluster ($HADOOP_HOME/bin/hdfs dfs -put <path_to_data>)

Run Wordcount
Example:
/usr/local/spark-1.5.1-bin-hadoop2.6/bin/spark-submit --jars flight_analysis.jar --class cs455.flightdata.spark.WordCount flight_analysis.jar hdfs://salem.cs.colostate.edu:50000/data/flight_data

Note:
Running wordCount on the Flight Data hasn't been successful yet; currently looking