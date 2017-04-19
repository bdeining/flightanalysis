package cs455.flightdata.spark;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*

    Name	Description
    1	Year	1987-2008
    2	Month	1-12
    3	DayofMonth	1-31
    4	DayOfWeek	1 (Monday) - 7 (Sunday)
    5	DepTime	actual departure time (local, hhmm)
    6	CRSDepTime	scheduled departure time (local, hhmm)
    7	ArrTime	actual arrival time (local, hhmm)
    8	CRSArrTime	scheduled arrival time (local, hhmm)
    9	UniqueCarrier	unique carrier code
    10	FlightNum	flight number
    11	TailNum	plane tail number
    12	ActualElapsedTime	in minutes
    13	CRSElapsedTime	in minutes
    14	AirTime	in minutes
    15	ArrDelay	arrival delay, in minutes
    16	DepDelay	departure delay, in minutes
    17	Origin	origin IATA airport code
    18	Dest	destination IATA airport code
    19	Distance	in miles
    20	TaxiIn	taxi in time, in minutes
    21	TaxiOut	taxi out time in minutes
    22	Cancelled	was the flight cancelled?
    23	CancellationCode	reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
    24	Diverted	1 = yes, 0 = no
    25	CarrierDelay	in minutes
    26	WeatherDelay	in minutes
    27	NASDelay	in minutes
    28	SecurityDelay	in minutes
    29	LateAircraftDelay	in minutes

    1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA, 0,NA ,0,NA,NA,NA,NA,NA
    1   , 2, 3,4,  5,  6,  7,  8, 9,  10,11,12,13,14,15,16, 17, 18, 19,20,21,22,23,24,25,27,27,28,29
 */

public class FlightDataAnalysis {

    private static final Pattern COMMA = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: FlightDataAnalysis <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("FlightDataAnalysis");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaPairRDD<String, Long> commercialFlightDelay = lines.mapToPair(string -> {
            String[] flightData = string.split(COMMA.pattern());
            try {
                Long integer = Long.parseLong(flightData[15]);
                return new Tuple2<>(flightData[0], integer);
            } catch (NumberFormatException e) {
                return new Tuple2<>(flightData[0], 0L);
            }
        });

        JavaPairRDD<String, Long> reducedCommercialFlightDelay = commercialFlightDelay.reduceByKey((long1, long2) -> (
                long1 + long2));

        List<Tuple2<String, Long>> totalYearlyFlightDelayInMinutes = reducedCommercialFlightDelay.collect();
        for (Tuple2<String, Long> tuple : totalYearlyFlightDelayInMinutes) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        ctx.stop();
    }

}
