### Authors
* Ben Deininger
* Nicholas Malensek
* David Edwards
* Stephen Rhoads

### For Local Development :
Download Spark 1.5.1 for Hadoop 2.6 (http://spark.apache.org/downloads.html)
Add Jars to IDE

### For Development in CS Lab :
cs computers - spark-1.5.1-bin-hadoop2.6

### Flight Data :
http://stat-computing.org/dataexpo/2009/the-data.html

Stage Data in Local Hadoop Cluster ($HADOOP_HOME/bin/hdfs dfs -put <path_to_data>)

The script load_flight_data_into_hdfs.sh loads all of the data into hdfs. The only modification the script
may need is the destination for the files.

For doing plane model analysis, an additional data set is needed.  Download the aircraft registry 'database' from
https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download/ .
Two files are needed from this set, MASTER.txt and ACFTREF.txt, and both will need the first row removed before being used.
The first row contains column headers, and they aren't needed for the spark processing.

Once these files are acquired and modified, put them into hdfs, but in a different folder than the flight data.

### Run Flight Data Analysis
Add /usr/local/spark-1.5.1-bin-hadoop2.6/bin/ to path.

Flight Cancellation: all time cancellation counts by cancellation code, cancellation reasons over time and cancellation counts by carrier code

    spark-submit --master yarn --class cs455.flightdata.spark.flight_cancellation.FlightCancellation flight_analysis.jar <flight-data-input-dir> <output-dir>

Distance Impact: counts for delays and cancellations by distance grouping of 500 miles (0-500, 501-1000.... >5000)

     spark-submit --master yarn --class cs455.flightdata.spark.distance_impact.DistanceImpact flight_analysis.jar <flight-data-input-dir> <output-dir> 

Impacts of Active Airlines on Delays: counts by each month on how many flights occured, how many were delayed, and how many airlines flew at least once that month.
Also provides information on delays by aircraft model.
   
     spark-submit --master yarn --class cs455.flightdata.spark.number_airlines_impact.NumberOfAirlinesDelay flight_analysis.jar <flight-data-input-dir> <output-dir> <master-file-location> <ACFTREF-file-location>

Delays by year, day of the week, day of the month

     spark-submit --master yarn --class cs455.flightdata.spark.FlightDelayAnalysis flight_analysis.jar <flight-data-input-dir> <output-dir>

Delays by Aircraft Model

     spark-submit --master yarn --class cs455.flightdata.spark.TailNumberDelays flight_analysis.jar <flight-data-input-dir> <output-dir> <master-file-location> <ACFTREF-file-location>
    
Top 50 count, average delay in minutes, and percentage of delays by aircraft model

     spark-submit --master yarn --class cs455.flightdata.spark.DelayByModel flight_analysis.jar <flight-data-input-dir> <output-dir> <master-file-location> <ACFTREF-file-location>

Delays by airport, outputs top and bottom 50 delayed airports by count and percentage, and average delays (in minutes) across all airports.

     spark-submit --master yarn --class cs455.flightdata.spark.AirportAnalysis flight_analysis.jar <flight-data-input-dir> <output-dir>
    
Delays on Daylight Saving crossover days, outputs the average delay (in minutes) for delayed DST flights per year and the percentage of DST flights that are delayed per year

     spark-submit --master yarn --class cs455.flightdata.spark.DSTAnalysis flight_analysis.jar <flight-data-input-dir> <output-dir>

Average Delays, and delay counts.

     spark-submit --master yarn --class cs455.flightdata.spark.FlightDelayReasons flight_analysis.jar <flight-data-input-dir> <output-dir>


### Visualization

The csv and json files are parsed, lightly processed, and turned into graphs that can be viewed here: http://cs455.dev-cycle.com/

Code for this site can be found in /FlightDataWeb, and ([https://github.com/doofusdavid/FlightDataWeb](https://github.com/doofusdavid/FlightDataWeb))
