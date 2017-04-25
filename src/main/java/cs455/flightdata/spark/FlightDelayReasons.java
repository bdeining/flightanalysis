package cs455.flightdata.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
/*
 -
 -    Name	Description
 -    1	Year	1987-2008
 -    2	Month	1-12
 -    3	DayofMonth	1-31
 -    4	DayOfWeek	1 (Monday) - 7 (Sunday)
 -    5	DepTime	actual departure time (local, hhmm)
 -    6	CRSDepTime	scheduled departure time (local, hhmm)
 -    7	ArrTime	actual arrival time (local, hhmm)
 -    8	CRSArrTime	scheduled arrival time (local, hhmm)
 -    9	UniqueCarrier	unique carrier code
 -    10	FlightNum	flight number
 -    11	TailNum	plane tail number
 -    12	ActualElapsedTime	in minutes
 -    13	CRSElapsedTime	in minutes
 -    14	AirTime	in minutes
 -    15	ArrDelay	arrival delay, in minutes
 -    16	DepDelay	departure delay, in minutes
 -    17	Origin	origin IATA airport code
 -    18	Dest	destination IATA airport code
 -    19	Distance	in miles
 -    20	TaxiIn	taxi in time, in minutes
 -    21	TaxiOut	taxi out time in minutes
 -    22	Cancelled	was the flight cancelled?
 -    23	CancellationCode	reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
 -    24	Diverted	1 = yes, 0 = no
 -    25	CarrierDelay	in minutes
 -    26	WeatherDelay	in minutes
 -    27	NASDelay	in minutes
 -    28	SecurityDelay	in minutes
 -    29	LateAircraftDelay	in minutes
 -
 -    1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA, 0,NA ,0,NA,NA,NA,NA,NA
 -    1   , 2, 3,4,  5,  6,  7,  8, 9,  10,11,12,13,14,15,16, 17, 18, 19,20,21,22,23,24,25,27,27,28,29
 - */
public class FlightDelayReasons
{
    private static final Pattern COMMA = Pattern.compile(",");
    private static final int CANCELLATION_CODE_INDEX = 22;
    private static final int DELAY_TIME_MINUTES_INDEX = 15;
    private static final int YEAR_INDEX = 0;
    private static final int CARRIER_DELAY_MINUTES_INDEX = 24;
    private static final int WEATHER_DELAY_MINUTES_INDEX = 25;
    private static final int NAS_DELAY_MINUTES_INDEX = 26;
    private static final int SECURITY_DELAY_MINUTES_INDEX = 27;
    private static final int LATEAIRCRAFT_DELAY_MINUTES_INDEX = 28;

    private static List<String> reasons;

    public static void main(String[] args)
    {
        if (args.length != 2)
        {
            System.err.println("Usage: FlightDelayReasons <input-directory> <output-directory>");
            System.exit(1);
        }

        String outputDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("FlightDelayReasons");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        processFlightCancellationReasons(lines, outputDir);
        processFlightDelayAverageDelay(lines, outputDir);
        processFlightDelayCounts(lines, outputDir);

        ctx.stop();
    }

    private static void processFlightDelayCounts(JavaRDD<String> lines, String outputDir)
    {

        JavaPairRDD<String, Long> carrierCount = lines.mapToPair(string -> processGenericCount(string, CARRIER_DELAY_MINUTES_INDEX, "Carrier Issues"));
        JavaPairRDD<String, Long> weatherCount = lines.mapToPair(string -> processGenericCount(string, WEATHER_DELAY_MINUTES_INDEX, "Carrier Issues"));
        JavaPairRDD<String, Long> nasCount = lines.mapToPair(string -> processGenericCount(string, NAS_DELAY_MINUTES_INDEX, "Carrier Issues"));
        JavaPairRDD<String, Long> securityCount = lines.mapToPair(string -> processGenericCount(string, SECURITY_DELAY_MINUTES_INDEX, "Carrier Issues"));
        JavaPairRDD<String, Long> lateAircraftCount = lines.mapToPair(string -> processGenericCount(string, LATEAIRCRAFT_DELAY_MINUTES_INDEX, "Carrier Issues"));
        JavaPairRDD<String, Long> allDelays = carrierCount.union(weatherCount).union(nasCount).union(securityCount).union(lateAircraftCount);

        SparkUtils.saveCoalescedRDDToJsonFile(allDelays,
                outputDir + File.separator + "flight_delays_by_reason");

    }

    private static void processFlightCancellationReasons(JavaRDD<String> lines, String outputDir)
    {
        JavaPairRDD<String, Integer> commercialFlightDelay = lines.mapToPair(string ->
        {
            String[] flightData = string.split(COMMA.pattern());
            try
            {
                return new Tuple2<>(flightData[CANCELLATION_CODE_INDEX], 1);
            }
            catch (NumberFormatException e)
            {
                return new Tuple2<>(flightData[CANCELLATION_CODE_INDEX], 0);
            }
        });

        JavaPairRDD<String, Integer> reducedReasons = commercialFlightDelay.reduceByKey((int1, int2) -> (int1 + int2));
        SparkUtils.saveCoalescedRDDToJsonFile(reducedReasons,
                outputDir + File.separator + "flight_cancellation_reasons");

    }

    private static void processFlightDelayAverageDelay(JavaRDD<String> lines, String outputDir)
    {

        JavaPairRDD<String,Tuple2<Long,Long>> averageCarrierDelay = processDelay(lines, CARRIER_DELAY_MINUTES_INDEX, "Carrier Delay");
        JavaPairRDD<String,Tuple2<Long,Long>> averageWeatherDelay = processDelay(lines, WEATHER_DELAY_MINUTES_INDEX, "Weather Delay");
        JavaPairRDD<String,Tuple2<Long,Long>> averageNASDelay = processDelay(lines, NAS_DELAY_MINUTES_INDEX, "National Airspace System Delay");
        JavaPairRDD<String,Tuple2<Long,Long>> averageSecurityDelay = processDelay(lines, SECURITY_DELAY_MINUTES_INDEX, "Security Delay");
        JavaPairRDD<String,Tuple2<Long,Long>> averageLateAircraftDelay = processDelay(lines, LATEAIRCRAFT_DELAY_MINUTES_INDEX, "Late Aircraft Delay");

        JavaPairRDD<String, Tuple2<Long, Long>> allDelays = averageCarrierDelay.union(averageWeatherDelay).union(averageNASDelay).union(averageSecurityDelay).union(averageLateAircraftDelay);

        SparkUtils.saveCoalescedRDDToJsonFile(allDelays,
                outputDir + File.separator + "flight_average_delay_by_reason");
    }

    private static JavaPairRDD<String,Tuple2<Long,Long>> processDelay(JavaRDD<String> lines, int index, String dataName)
    {
        JavaPairRDD<String, Long> flightDelay = lines.mapToPair(string -> processGenericDelay(string, index, dataName));

        JavaPairRDD<String, Long> reducedFlightDelay = flightDelay.reduceByKey((long1, long2) -> (long1 + long2));

        JavaPairRDD<String, Long> delayCount = lines.mapToPair(string->processGenericCount(string, index, dataName));
        JavaPairRDD<String, Long> reducedDelayCount = delayCount.reduceByKey((long1, long2) -> (long1 + long2));

        JavaPairRDD<String, Tuple2<Long, Long>> averageCarrierCounts = reducedFlightDelay.join(
                reducedDelayCount);

        return averageCarrierCounts;
    }
    private static Tuple2<String, Long> processGenericDelay(String input, int dataPosition, String dataName)
    {
        String[] flightData = input.split(COMMA.pattern());

        try
        {
            Long delayTime = Long.parseLong(flightData[dataPosition]);
            return new Tuple2<>(flightData[dataPosition], delayTime);
        }
        catch (NumberFormatException e)
        {
            return new Tuple2<>(flightData[dataPosition], 0L);
        }

    }

    private static Tuple2<String, Long> processGenericCount(String input, int dataPosition, String dataName)
    {
        String[] flightData = input.split(COMMA.pattern());
        try
        {
            Long delayExists = Long.parseLong(flightData[dataPosition]);
            if(delayExists > 0)
                return new Tuple2<>(flightData[dataPosition], 1L);
            else
                return new Tuple2<>(flightData[dataPosition], 0L);
        }
        catch (NumberFormatException e)
        {
            return new Tuple2<>(flightData[dataPosition], 0L);
        }

    }
}