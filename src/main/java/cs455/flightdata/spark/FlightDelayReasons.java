package cs455.flightdata.spark;

import java.io.File;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class FlightDelayReasons {
    private static final Pattern COMMA = Pattern.compile(",");

    private static final int CANCELLATION_CODE_INDEX = 22;

    private static final int CARRIER_DELAY_MINUTES_INDEX = 24;

    private static final int WEATHER_DELAY_MINUTES_INDEX = 25;

    private static final int NAS_DELAY_MINUTES_INDEX = 26;

    private static final int SECURITY_DELAY_MINUTES_INDEX = 27;

    private static final int LATE_AIRCRAFT_DELAY_MINUTES_INDEX = 28;

    public static void main(String[] args) {
        if (args.length != 2) {
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

    private static void processFlightDelayCounts(JavaRDD<String> lines, String outputDir) {

        JavaPairRDD<String, Long> carrierCount = lines.mapToPair(string -> processGenericCount(
                string,
                CARRIER_DELAY_MINUTES_INDEX,
                "Carrier Issues"));
        JavaPairRDD<String, Long> weatherCount = lines.mapToPair(string -> processGenericCount(
                string,
                WEATHER_DELAY_MINUTES_INDEX,
                "Carrier Issues"));
        JavaPairRDD<String, Long> nasCount = lines.mapToPair(string -> processGenericCount(string,
                NAS_DELAY_MINUTES_INDEX,
                "Carrier Issues"));
        JavaPairRDD<String, Long> securityCount = lines.mapToPair(string -> processGenericCount(
                string,
                SECURITY_DELAY_MINUTES_INDEX,
                "Carrier Issues"));
        JavaPairRDD<String, Long> lateAircraftCount = lines.mapToPair(string -> processGenericCount(
                string, LATE_AIRCRAFT_DELAY_MINUTES_INDEX,
                "Carrier Issues"));
        JavaPairRDD<String, Long> allDelays = carrierCount.union(weatherCount)
                .union(nasCount)
                .union(securityCount)
                .union(lateAircraftCount);

        SparkUtils.saveCoalescedRDDToJsonFile(allDelays,
                outputDir + File.separator + "flight_delays_by_reason");

    }

    private static void processFlightCancellationReasons(JavaRDD<String> lines, String outputDir) {
        JavaPairRDD<String, Integer> commercialFlightDelay = lines.mapToPair(string -> {
            String[] flightData = string.split(COMMA.pattern());
            try {
                return new Tuple2<>(flightData[CANCELLATION_CODE_INDEX], 1);
            } catch (NumberFormatException e) {
                return new Tuple2<>(flightData[CANCELLATION_CODE_INDEX], 0);
            }
        });

        JavaPairRDD<String, Integer> reducedReasons =
                commercialFlightDelay.reduceByKey((int1, int2) -> (int1 + int2));
        SparkUtils.saveCoalescedRDDToJsonFile(reducedReasons,
                outputDir + File.separator + "flight_cancellation_reasons");

    }

    private static void processFlightDelayAverageDelay(JavaRDD<String> lines, String outputDir) {

        JavaPairRDD<String, Tuple2<Long, Long>> averageCarrierDelay = processDelay(lines,
                CARRIER_DELAY_MINUTES_INDEX,
                "Carrier Delay");
        JavaPairRDD<String, Tuple2<Long, Long>> averageWeatherDelay = processDelay(lines,
                WEATHER_DELAY_MINUTES_INDEX,
                "Weather Delay");
        JavaPairRDD<String, Tuple2<Long, Long>> averageNASDelay = processDelay(lines,
                NAS_DELAY_MINUTES_INDEX,
                "National Airspace System Delay");
        JavaPairRDD<String, Tuple2<Long, Long>> averageSecurityDelay = processDelay(lines,
                SECURITY_DELAY_MINUTES_INDEX,
                "Security Delay");
        JavaPairRDD<String, Tuple2<Long, Long>> averageLateAircraftDelay = processDelay(lines,
                LATE_AIRCRAFT_DELAY_MINUTES_INDEX,
                "Late Aircraft Delay");

        JavaPairRDD<String, Tuple2<Long, Long>> allDelays = averageCarrierDelay.union(
                averageWeatherDelay)
                .union(averageNASDelay)
                .union(averageSecurityDelay)
                .union(averageLateAircraftDelay);

        SparkUtils.saveCoalescedRDDToJsonFile(allDelays,
                outputDir + File.separator + "flight_average_delay_by_reason");
    }

    private static JavaPairRDD<String, Tuple2<Long, Long>> processDelay(JavaRDD<String> lines,
            int index, String dataName) {
        JavaPairRDD<String, Long> flightDelay =
                lines.mapToPair(string -> processGenericDelay(string, index, dataName));

        JavaPairRDD<String, Long> reducedFlightDelay = flightDelay.reduceByKey((long1, long2) -> (
                long1 + long2));

        JavaPairRDD<String, Long> delayCount = lines.mapToPair(string -> processGenericCount(string,
                index,
                dataName));
        JavaPairRDD<String, Long> reducedDelayCount = delayCount.reduceByKey((long1, long2) -> (
                long1 + long2));

        JavaPairRDD<String, Tuple2<Long, Long>> averageCarrierCounts = reducedFlightDelay.join(
                reducedDelayCount);

        return averageCarrierCounts;
    }

    private static Tuple2<String, Long> processGenericDelay(String input, int dataPosition,
            String dataName) {
        String[] flightData = input.split(COMMA.pattern());

        try {
            Long delayTime = Long.parseLong(flightData[dataPosition]);
            return new Tuple2<>(flightData[dataPosition], delayTime);
        } catch (NumberFormatException e) {
            return new Tuple2<>(flightData[dataPosition], 0L);
        }

    }

    private static Tuple2<String, Long> processGenericCount(String input, int dataPosition,
            String dataName) {
        String[] flightData = input.split(COMMA.pattern());
        try {
            Long delayExists = Long.parseLong(flightData[dataPosition]);
            if (delayExists > 0) {
                return new Tuple2<>(flightData[dataPosition], 1L);
            } else {
                return new Tuple2<>(flightData[dataPosition], 0L);
            }
        } catch (NumberFormatException e) {
            return new Tuple2<>(flightData[dataPosition], 0L);
        }

    }
}