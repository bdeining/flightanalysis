package cs455.flightdata.spark;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class DelayByModel {

    //found by sorting output and printing it
    private static final double PERCENT_TOP_50_LIMIT = 51.72;

    private static final double AVERAGE_TOP_50_LIMIT = 105.00;

    private static final int COUNT_TOP_50_LIMIT = 5719;


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

    public void analyze(String inputDir, String outputDir, JavaSparkContext context,
                        String tailNumberMasterFile, String manufacturerModelLookupFile) {
        JavaRDD<String> lines = context.textFile(inputDir);

        Map<String, String> tailNumMap = createTailNumMappingRdd(context, tailNumberMasterFile, manufacturerModelLookupFile);

        JavaRDD<String> onlyDelayed = lines.filter((Function<String, Boolean>) s -> {
                    try {
                        if (Integer.parseInt(s.split(",")[15]) > 15
                                && !s.split(",")[10].equals("NA")) {
                            return true;
                        } else {
                            return false;
                        }
                    } catch (NumberFormatException e) {
                        return false;
                    }
                });

        JavaRDD<String> allFlights = lines.filter((Function<String, Boolean>) s -> {
                    try {
                        Integer.parseInt(s.split(",")[15]);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                    return true;
                });

        /**
         * Delayed flights
         */

        JavaPairRDD<String, Long> modelTuple =
                onlyDelayed.mapToPair((PairFunction<String, String, Long>) mT -> {
                        String model = lookupTailNumber(mT.split(",")[10], tailNumMap);
                    return new Tuple2<>(model, 1L);
                });

        JavaPairRDD<String, Long> counts =
                modelTuple.reduceByKey((count1, count2) ->
                        count1 + count2);

        JavaPairRDD<String, Long> upperCounts =
                counts.filter((Tuple2<String, Long> countPair) -> countPair._2()
                        >= COUNT_TOP_50_LIMIT);

        SparkUtils.saveCoalescedRDDToJsonFile(upperCounts, outputDir + "/model-upper-count");

        List<Tuple2<String, Long>> output = counts.collect();

        Comparator<Tuple2<String, Long>> comparator = Comparator.comparing(Tuple2::_2);

        Collections.sort(output, comparator);

        /**
         * Average delays
         */

        JavaPairRDD<String, Tuple2<Long, Long>> combineRDDs =
                onlyDelayed.mapToPair((String line) -> {
                    String model = lookupTailNumber(line.split(",")[10], tailNumMap);
                    long delayAmount = Long.parseLong(line.split(",")[15]);
                    long averageTotal = 1L;
                    return new Tuple2<>(model, new Tuple2<>(delayAmount, averageTotal));
                })
                        .reduceByKey((Tuple2<Long, Long> one, Tuple2<Long, Long> two) ->
                        new Tuple2<>(one._1 + two._1, one._2 + two._2));

        JavaPairRDD<String, Double> answerRDD =
                combineRDDs.mapValues((Tuple2<Long, Long> tuple) -> new Double(
                        ((tuple._1) / (tuple._2))));

        JavaPairRDD<String, Double> filteredUpperAverage =
                answerRDD.filter((Tuple2<String, Double> averagePair) -> averagePair._2()
                        >= AVERAGE_TOP_50_LIMIT);

        SparkUtils.saveCoalescedRDDToJsonFile(filteredUpperAverage,
                outputDir + "/model-upper-average");

        List<Tuple2<String, Double>> averageList = answerRDD.collect();

        Comparator<Tuple2<String, Double>> averageCompare = Comparator.comparing(Tuple2::_2);

        Collections.sort(averageList, averageCompare);

        /**
         * Percent delays
         */

        JavaPairRDD<String, Tuple2<Long, Long>> percentRDD =
                allFlights.mapToPair((String s) -> {
                    String airplane = lookupTailNumber(s.split(",")[10], tailNumMap);
                    long delayedFlightCount = 0;
                    long total;
                    if (Integer.parseInt(s.split(",")[15]) > 15) {
                        delayedFlightCount = 1;
                        total = 1L;
                    } else {
                        total = 1L;
                    }
                    return new Tuple2<>(airplane, new Tuple2<> (delayedFlightCount, total));
                })
                        .reduceByKey((Tuple2<Long, Long> tupleOne, Tuple2<Long, Long> tupleTwo)
                                -> new Tuple2<>(tupleOne._1 + tupleTwo._1, tupleOne._2 + tupleTwo._2));

        JavaPairRDD<String, Double> percentModelDelayRDD =
                         percentRDD.mapValues((Tuple2<Long, Long> percentTuple)
                        -> new Double((percentTuple._1 * 100d) / (percentTuple._2)));

        JavaPairRDD<String, Double> filteredUpperModelDelayPercentage = percentModelDelayRDD.filter(
                (Tuple2<String, Double> upperPercentage) -> upperPercentage._2()
                        >= PERCENT_TOP_50_LIMIT);

        SparkUtils.saveCoalescedRDDToJsonFile(filteredUpperModelDelayPercentage,
                outputDir + "/model-upper-percent");

        List<Tuple2<String, Double>> percentList = percentModelDelayRDD.collect();

        Comparator<Tuple2<String, Double>> percentCompare = Comparator.comparing(Tuple2::_2);

        Collections.sort(percentList, percentCompare);

        /**
         * Print answers
         */
        System.out.println("*** A ***");
        System.out.println("Most:");
        for (int i = output.size() - 1; i > output.size() - 50; i--) {
            System.out.println(output.get(i)
                    ._1() + ":" + output.get(i)
                    ._2());
        }
        System.out.println("Least:");
        for (int i = 0; i < 50; i++) {
            System.out.println(output.get(i)
                    ._1() + ":" + output.get(i)
                    ._2());
        }

        System.out.println("*** B, average delay time by model ***");
        for (int i = averageList.size() - 1; i > averageList.size() - 50; i--) {
            System.out.println(averageList.get(i)
                    ._1() + ":" + averageList.get(i)
                    ._2());
        }

        System.out.println("*** C, percent delayed by model ***");
        for (int i = percentList.size() - 1; i > percentList.size() - 50; i--) {
            System.out.println(percentList.get(i)
                    ._1() + ":" + percentList.get(i)
                    ._2());
        }
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

    public static void main(String[] args) {
        DelayByModel delayByModel = new DelayByModel();
        SparkConf sparkConf = new SparkConf().setAppName("tp-model-analysis");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        delayByModel.analyze(args[0], args[1], context, args[2], args[3]);

        context.stop();
    }
}
