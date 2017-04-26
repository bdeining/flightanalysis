package cs455.flightdata.spark;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFlatMapFunction;
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
        processFlightDelayCounts(lines, outputDir);
        processAverageFlightDelay(lines, outputDir);

        ctx.stop();
    }

    private static void processFlightDelayCounts(JavaRDD<String> lines, String outputDir) {

        JavaPairRDD<String, Long> allCounts = processAllDelayCounts(lines);
        JavaPairRDD<String, Long> reducedCounts = allCounts.reduceByKey((x,y) -> x+y);


        SparkUtils.saveCoalescedRDDToJsonFile(reducedCounts,
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

    private static void processAverageFlightDelay(JavaRDD<String> lines, String outputDir)
    {

        JavaPairRDD<String, Tuple2<Long,Long>> allDelays = processAllDelays(lines);
        JavaPairRDD<String, Tuple2<Long,Long>> reducedDelays = allDelays.reduceByKey((x,y) -> new Tuple2<>((x._1+y._1),(x._2+y._2)));


        SparkUtils.saveCoalescedRDDToJsonFile(reducedDelays,
                outputDir + File.separator + "flight_average_delay_by_reason");

    }

    private static JavaPairRDD<String, Tuple2<Long, Long>> processAllDelays(JavaRDD<String> lines)
    {

        JavaPairRDD<String,Tuple2<Long,Long>> delays = lines.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<Long, Long>>()
        {
            @Override
            public Iterable<Tuple2<String, Tuple2<Long, Long>>> call(String s) throws Exception
            {
                String[] flightData = s.split(COMMA.pattern());
                List<Tuple2<String, Tuple2<Long, Long>>> results = new ArrayList<>();
                results.add(getSingleReason(flightData[CARRIER_DELAY_MINUTES_INDEX],"Carrier Delay"));
                results.add(getSingleReason(flightData[WEATHER_DELAY_MINUTES_INDEX], "Weather Delay"));
                results.add(getSingleReason(flightData[NAS_DELAY_MINUTES_INDEX],"NAS Delay"));
                results.add(getSingleReason(flightData[SECURITY_DELAY_MINUTES_INDEX], "Security Delay"));
                results.add(getSingleReason(flightData[LATE_AIRCRAFT_DELAY_MINUTES_INDEX],"Late Aircraft Delay"));
                return results;
            }
        });

        return delays;
    }

    private static JavaPairRDD<String, Long> processAllDelayCounts(JavaRDD<String> lines)
    {
        JavaPairRDD<String, Long> counts = lines.flatMapToPair(new PairFlatMapFunction<String, String, Long>()
        {
            @Override
            public Iterable<Tuple2<String, Long>> call(String s) throws Exception
            {
                String[] flightData = s.split(COMMA.pattern());
                List<Tuple2<String,Long>> results = new ArrayList<>();
                results.add(processGenericCount(flightData[CARRIER_DELAY_MINUTES_INDEX],"Carrier Delay"));
                results.add(processGenericCount(flightData[WEATHER_DELAY_MINUTES_INDEX], "Weather Delay"));
                results.add(processGenericCount(flightData[NAS_DELAY_MINUTES_INDEX],"NAS Delay"));
                results.add(processGenericCount(flightData[SECURITY_DELAY_MINUTES_INDEX], "Security Delay"));
                results.add(processGenericCount(flightData[LATE_AIRCRAFT_DELAY_MINUTES_INDEX],"Late Aircraft Delay"));
                return results;
            }
        });
        return counts;
    }

    private static Tuple2<String,Tuple2<Long,Long>> getSingleReason(String data, String dataName)
    {
        try
        {
            Long delayTime = Long.parseLong(data);
            return new Tuple2<>(dataName, new Tuple2<>(delayTime,1L));
        }
        catch (NumberFormatException e)
        {
            return new Tuple2<>(dataName, new Tuple2<>(0L,0L));
        }

    }


    private static Tuple2<String, Long> processGenericCount(String input,
            String dataName) {
        try {
            Long delayExists = Long.parseLong(input);
            if (delayExists > 0) {
                return new Tuple2<>(dataName, 1L);
            } else {
                return new Tuple2<>(dataName, 0L);
            }
        } catch (NumberFormatException e) {
            return new Tuple2<>(dataName, 0L);
        }

    }
}