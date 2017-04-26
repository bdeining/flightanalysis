package cs455.flightdata.spark;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import cs455.flightdata.spark.flight_cancellation.FlightCancellation;
import cs455.flightdata.spark.number_airlines_impact.NumberOfAirlinesDelay;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

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

        SparkUtils.saveCoalescedTupleRDDToCsvFile(flightCounts,
                outputDir + File.separator + "flight_delay_per_number_of_flights");

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

        processFlightDelaysGeneric(lines,
                dayOfWeek,
                outputDir + File.separator + "flight_delay_per_day_of_week",
                DAY_OF_WEEK_INDEX);
    }

    private static void processFlightDelaysPerDayOfTheMonth(JavaRDD<String> lines,
            String outputDir) {
        processFlightDelaysGeneric(lines,
                null,
                outputDir + File.separator + "flight_delay_per_day_of_month",
                DAY_OF_MONTH_INDEX);
    }

    private static void processFlightDelaysPerMonth(JavaRDD<String> lines, String outputDir) {
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
                "December");
        processFlightDelaysGeneric(lines,
                months,
                outputDir + File.separator + "flight_delay_per_month",
                MONTH_INDEX);
    }

    private static void processFlightDelaysGeneric(JavaRDD<String> lines,
            List<String> optionalStringMapping, String outputDir, int index) {

        JavaPairRDD<String, Long> commercialFlightDelayCount = lines.mapToPair(string -> {
            String[] flightData = string.split(COMMA.pattern());

            if (optionalStringMapping != null) {
                Integer stringIndex = Integer.parseInt(flightData[index]);
                if (hasDelay(flightData)) {
                    return new Tuple2<>(optionalStringMapping.get(stringIndex - 1), 1L);
                } else {
                    return new Tuple2<>(optionalStringMapping.get(stringIndex - 1), 0L);
                }
            } else {
                if (hasDelay(flightData)) {
                    return new Tuple2<>(flightData[index], 1L);
                } else {
                    return new Tuple2<>(flightData[index], 0L);
                }
            }
        });

        JavaPairRDD<String, Long> reducedCommercialFlightDelayCount =
                commercialFlightDelayCount.reduceByKey((integer1, integer2) -> (integer1
                        + integer2));

        SparkUtils.saveCoalescedLongRDDToCsvFile(reducedCommercialFlightDelayCount, outputDir);

        SparkUtils.saveCoalescedRDDToJsonFile(reducedCommercialFlightDelayCount, outputDir);
    }

    private static boolean hasDelay(String[] flightData) {
        try {
            Long delayTime = Long.parseLong(flightData[DELAY_TIME_MINUTES_INDEX]);
            return delayTime > 15;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
