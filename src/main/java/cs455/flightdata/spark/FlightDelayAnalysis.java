package cs455.flightdata.spark;

import java.io.File;
import java.util.Arrays;
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

/*
    Delay per day of week
    Delay per day of month
    Delay per month of year
    Flight Delays Per Year Percentage

    The most common cause for flight cancellations
    Daylight Savings crossover dates
    Airport analysis of flight delays
    Airports with most and fewest delays

    Average delay by airport
    Flight delay reasons
    Most common reasons for flight delays
    Average time delay for each delay reason

    Airplane model delays
    Airplane models with most and fewest delays
    Impacts of mergers on delays and cancellations
    Impact of flight distance/destination (ex. Flights to Europe) on delay frequency

*/

public class FlightDelayAnalysis {

    private static final Pattern COMMA = Pattern.compile(",");

    private static final int DELAY_TIME_MINUTES_INDEX = 15;

    private static final int MONTH_INDEX = 1;

    private static final int DAY_OF_MONTH_INDEX = 2;

    private static final int DAY_OF_WEEK_INDEX = 3;

    private static final int YEAR_INDEX = 0;

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: FlightDelayAnalysis <input-directory> <output-directory>");
            System.exit(1);
        }

        String outputDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("FlightDelayAnalysis");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        processFlightDelayPerNumberOfFlights(lines, outputDir);
        processFlightDelaysPerDayOfTheWeek(lines, outputDir);
        processFlightDelaysPerDayOfTheMonth(lines, outputDir);
        processFlightDelaysPerMonth(lines, outputDir);

        ctx.stop();
    }

    private static void processFlightDelayPerNumberOfFlights(JavaRDD<String> lines,
            String outputDir) {
        JavaPairRDD<String, Long> commercialFlightDelay = lines.mapToPair(string -> {
            String[] flightData = string.split(COMMA.pattern());
            try {
                Long delayTime = Long.parseLong(flightData[DELAY_TIME_MINUTES_INDEX]);
                return new Tuple2<>(flightData[YEAR_INDEX], delayTime > 15 ? delayTime : 0);
            } catch (NumberFormatException e) {
                return new Tuple2<>(flightData[YEAR_INDEX], 0L);
            }
        });

        JavaPairRDD<String, Long> numberOfFlights = lines.mapToPair((string) -> {
            String[] flightData = string.split(COMMA.pattern());
            return new Tuple2<>(flightData[YEAR_INDEX], 1L);
        });

        JavaPairRDD<String, Long> reducedNumberOfFlights =
                numberOfFlights.reduceByKey((long1, long2) -> (long1 + long2));

        JavaPairRDD<String, Long> reducedCommercialFlightDelay =
                commercialFlightDelay.reduceByKey((long1, long2) -> (long1 + long2));

        JavaPairRDD<String, Tuple2<Long, Long>> flightCounts = reducedCommercialFlightDelay.join(
                reducedNumberOfFlights);

        SparkUtils.saveCoalescedRDDToJsonFile(flightCounts,
                outputDir + File.separator + "flight_delay_per_number_of_flights");
    }

    private static void processFlightDelaysPerDayOfTheWeek(JavaRDD<String> lines,
            String outputDir) {
        List<String> dayOfWeek = Arrays.asList("Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday");

        processFlightDelaysGeneric(lines, dayOfWeek,
                outputDir + File.separator + "flight_delay_per_day_of_week",
                DAY_OF_WEEK_INDEX);
    }

    private static void processFlightDelaysPerDayOfTheMonth(JavaRDD<String> lines,
            String outputDir) {
        processFlightDelaysGeneric(lines, null, outputDir + File.separator + "flight_delay_per_day_of_month", DAY_OF_MONTH_INDEX);
    }

    private static void processFlightDelaysPerMonth(JavaRDD<String> lines,
            String outputDir) {
        List<String> months = Arrays.asList("January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December"
        );
        processFlightDelaysGeneric(lines, months, outputDir + File.separator + "flight_delay_per_month", MONTH_INDEX);
    }


    private static void processFlightDelaysGeneric(JavaRDD<String> lines, List<String> optionalStringMapping,
            String outputDir, int index) {

        JavaPairRDD<String, Long> commercialFlightDelayCount = lines.mapToPair(string -> {
            String[] flightData = string.split(COMMA.pattern());

            if (optionalStringMapping != null) {
                Integer stringIndex = Integer.parseInt(flightData[index]);
                return new Tuple2<>(optionalStringMapping.get(stringIndex - 1), 1L);
            } else {
                return new Tuple2<>(flightData[index], 1L);
            }
        });

        JavaPairRDD<String, Long> reducedCommercialFlightDelayCount =
                commercialFlightDelayCount.reduceByKey((integer1, integer2) -> (integer1
                        + integer2));

        //SparkUtils.saveCoalescedRDDToTextFile(reducedCommercialFlightDelayCount, outputDir);
        SparkUtils.saveCoalescedRDDToJsonFile(reducedCommercialFlightDelayCount, outputDir);
    }

}
