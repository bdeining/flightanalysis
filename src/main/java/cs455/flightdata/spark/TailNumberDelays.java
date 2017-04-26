package cs455.flightdata.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TailNumberDelays {

    private static final int TAIL_NUM_INDEX = 10;

    private static final Pattern COMMA = Pattern.compile(",");

    private static final int DELAY_TIME_MINUTES_INDEX = 15;

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println(
                    "Usage: TailNumberDelays <input-directory> <output-directory> <tail-num-master_file> <manufacturer-model-lookup_file>");
            System.exit(1);
        }

        String outputDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("FlightDelayAnalysis");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        Map<String, String> tailNumMappings = createTailNumMappingRdd(ctx, args[2], args[3]);

        JavaPairRDD<String, Long> delayByAircraft = lines.mapToPair(string -> {
            String[] flightData = string.split(COMMA.pattern());
            String aircraftType = lookupTailNumber(flightData[TAIL_NUM_INDEX], tailNumMappings);
            if (hasDelay(flightData)) {
                return new Tuple2<>(aircraftType, 1L);
            } else {
                return new Tuple2<>(aircraftType, 0L);
            }
        });

        JavaPairRDD<String, Long> reducedDelayByAircraft =
                delayByAircraft.reduceByKey((integer1, integer2) -> (integer1 + integer2));

        SparkUtils.saveCoalescedLongRDDToCsvFile(reducedDelayByAircraft, outputDir);
        SparkUtils.saveCoalescedRDDToJsonFile(reducedDelayByAircraft, outputDir);
        ctx.stop();
    }

    private static String lookupTailNumber(String tailNumber, Map<String, String> tailNumMappings) {
        if (tailNumber.startsWith("N")) {
            tailNumber = tailNumber.replace("N", "");
        }

        String result = tailNumMappings.get(tailNumber);
        if (result != null) {
            return result;
        } else {
            return "Unavailable";
        }
    }

    private static Map<String, String> createTailNumMappingRdd(JavaSparkContext ctx,
            String tailNumMasterFile, String manufacturerModelLookupFile) {

        JavaRDD<String> tailNumRdd = ctx.textFile(tailNumMasterFile, 1);
        JavaRDD<String> mfrModelLookupRdd = ctx.textFile(manufacturerModelLookupFile, 1);

        // create rdd that's just tailnum and lookup field
        JavaPairRDD<String, String> tailNumMfrModelCodeRdd =
                tailNumRdd.mapToPair((PairFunction<String, String, String>) fullLine -> {
                    String nNumber = fullLine.substring(0, 5);
                    String mfrModelCode = fullLine.substring(37, 43);

                    return new Tuple2<>(mfrModelCode, nNumber);
                });

        // create rdd from actref that maps mfrModelCode to Mfr and model strings
        JavaPairRDD<String, String> codeToModelStringsRdd =
                mfrModelLookupRdd.mapToPair((PairFunction<String, String, String>) fullLine -> {
                    String mfrModelCode = fullLine.substring(0, 6);
                    //                    String mfrString = fullLine.substring(8, 37).trim();
                    String modelString = fullLine.substring(39, 58)
                            .trim();

                    return new Tuple2<>(mfrModelCode, modelString);
                });

        // join
        JavaPairRDD<String, Tuple2<String, String>> codeToMfrModelTailRdd =
                codeToModelStringsRdd.join(tailNumMfrModelCodeRdd);

        // tailnum to mfrModelCode
        JavaPairRDD<String, String> tailNumToMfr =
                codeToMfrModelTailRdd.mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) valTup -> new Tuple2<>(
                        valTup._2()
                                ._2(),
                        valTup._2()
                                ._1()));

        Map<String, String> tailNumberMap = new HashMap<>();
        List<Tuple2<String, String>> tailNumbers = tailNumToMfr.collect();
        for (Tuple2<String, String> tuple2 : tailNumbers) {
            tailNumberMap.put(tuple2._1(), tuple2._2());
        }
        return tailNumberMap;
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
