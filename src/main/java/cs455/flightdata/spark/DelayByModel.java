package cs455.flightdata.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class DelayByModel {

    //found by sorting output and printing it
    private static final double PERCENT_TOP_50_LIMIT = 51.72;

    private static final double AVERAGE_TOP_50_LIMIT = 105.00;

    private static final int COUNT_TOP_50_LIMIT = 5719;

    private static long AVERAGE_COUNT;

    private static double PERCENT_COUNT;


    public JavaPairRDD<String, String> createTailNumMappingRdd(JavaSparkContext ctx,
                                                               String tailNumMasterFile, String manufacturerModelLookupFile) {

        JavaRDD<String> tailNumRdd = ctx.textFile(tailNumMasterFile, 1);
        JavaRDD<String> mfrModelLookupRdd = ctx.textFile(manufacturerModelLookupFile, 1);

        // create rdd that's just tailnum and lookup field
        JavaPairRDD<String, String> tailNumMfrModelCodeRdd =
                tailNumRdd.mapToPair((PairFunction<String, String, String>) fullLine -> {
                    String nNumber = fullLine.substring(0, 4);
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
                codeToMfrModelTailRdd.mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>)
                        valTup -> new Tuple2<>(
                        valTup._2()
                                ._2(),
                        valTup._2()
                                ._1()));

        return tailNumToMfr;

    }

    public void analyze(String inputDir, String outputDir, JavaSparkContext context,
                        JavaPairRDD<String, String> tailNumberToModel) {
        JavaPairRDD<String, String> countNumberToModel = tailNumberToModel;
        JavaPairRDD<String, String> avgNumberToModel = tailNumberToModel;
        JavaPairRDD<String, String> percentNumberToModel = tailNumberToModel;
        JavaRDD<String> lines = context.textFile(inputDir);

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

        JavaPairRDD<String, Integer> modelTuple =
                onlyDelayed.mapToPair((PairFunction<String, String, Integer>) mT -> {
                        String tailNum = mT.split(",")[10];
                        if (tailNum != null && tailNum.length() >= 2) {
                            if (tailNum.charAt(0) == 'N') {
                                tailNum = tailNum.substring(1, tailNum.length());
                            }
                        }
                    return new Tuple2<>(tailNum, 1);
                });

        JavaPairRDD<String, Integer> counts =
                modelTuple.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) ->
                        integer + integer2);

        JavaPairRDD<String, Tuple2<Integer, String>> joinTailsAndModels =
                counts.join(countNumberToModel);

        JavaPairRDD<String, Integer> replaceTailNumWithModel =
                joinTailsAndModels.mapToPair((PairFunction<Tuple2<String, Tuple2<Integer, String>>, String, Integer>)
                tailNumber -> new Tuple2<>(
                        tailNumber._2()._2(),
                        tailNumber._2()._1()))
                .reduceByKey((l1, l2) -> l1 + l2);

        JavaPairRDD<String, Integer> upperCounts =
                replaceTailNumWithModel.filter((Tuple2<String, Integer> countPair) -> countPair._2()
                        >= COUNT_TOP_50_LIMIT);

        SparkUtils.saveCoalescedRDDToJsonFile(upperCounts, outputDir + "/model-upper-count");

        List<Tuple2<String, Integer>> output = counts.collect();

        Comparator<Tuple2<String, Integer>> comparator = Comparator.comparing(Tuple2::_2);

        Collections.sort(output, comparator);

        /**
         * Average delays
         */

        JavaPairRDD<String, Long> combineRDDs =
                onlyDelayed.mapToPair((String line) -> {
                    String model = line.split(",")[10];
                    if (model != null && model.length() >= 2) {
                        if (model.charAt(0) == 'N') {
                            model = model.substring(1, model.length());
                        }
                    }
                    long delayAmount = Long.parseLong(line.split(",")[15]);
                    AVERAGE_COUNT += 1L;
                    return new Tuple2<>(model, delayAmount);
                })
                        .reduceByKey((t1, t2) -> (t1 + t2));

        JavaPairRDD<String, Double> answerRDD =
                combineRDDs.mapValues((tup1) -> new Double(
                        ((tup1) / (AVERAGE_COUNT))));

        JavaPairRDD<String, Tuple2<Double, String>> averageJoin = answerRDD.join(
                avgNumberToModel);

        JavaPairRDD<String, Double> avgTailNumToModel =
                averageJoin.mapToPair((PairFunction<Tuple2<String, Tuple2<Double, String>>, String, Double>) tail ->
                new Tuple2<>(tail._2()
                ._2(), tail._2()._1()))
                .reduceByKey((t1, t2) -> t1 + t2);

        JavaPairRDD<String, Double> filteredUpperAverage =
                avgTailNumToModel.filter((Tuple2<String, Double> averagePair) -> averagePair._2()
                        >= AVERAGE_TOP_50_LIMIT);

        SparkUtils.saveCoalescedRDDToJsonFile(filteredUpperAverage,
                outputDir + "/model-upper-average");

        List<Tuple2<String, Double>> averageList = answerRDD.collect();

        Comparator<Tuple2<String, Double>> averageCompare = Comparator.comparing(Tuple2::_2);

        Collections.sort(averageList, averageCompare);

        /**
         * Percent delays
         */

        JavaPairRDD<String, Double> percentRDD =
                allFlights.mapToPair((String s) -> {
                    String airplane = s.split(",")[10];
                    if (airplane != null && airplane.length() >= 2) {
                        if (airplane.charAt(0) == 'N') {
                            airplane = airplane.substring(1, airplane.length());
                        }
                    }
                    double delayedFlightCount = 0;
                    if (Integer.parseInt(s.split(",")[15]) > 15) {
                        delayedFlightCount = 1;
                        PERCENT_COUNT += 1;
                    } else {
                        PERCENT_COUNT += 1;
                    }
                    return new Tuple2<>(airplane, delayedFlightCount);
                })
                        .reduceByKey((per1, per2) -> per1 + per2);

        JavaPairRDD<String, Double> percentModelDelayRDD =
                percentRDD.mapValues((percent1) -> ((percent1) / (PERCENT_COUNT)));

        JavaPairRDD<String, Tuple2<Double, String>> idToModel =
                percentModelDelayRDD.join(percentNumberToModel);

        JavaPairRDD<String, Double> tailNumbersIntoModels =
                idToModel.mapToPair((PairFunction<Tuple2<String, Tuple2<Double, String>>, String, Double>) percentTup ->
                new Tuple2<>(
                        percentTup._2()
                        ._2(),
                        percentTup._2()
                        ._1()))
                .reduceByKey((p1, p2) -> p1 + p2);

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

    public static void main(String[] args) {
        DelayByModel delayByModel = new DelayByModel();
        SparkConf sparkConf = new SparkConf().setAppName("tp-model-analysis");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> tailNumberToModel =
                delayByModel.createTailNumMappingRdd(context, args[2], args[3]);
        delayByModel.analyze(args[0], args[1], context, tailNumberToModel);

        context.stop();
    }
}
