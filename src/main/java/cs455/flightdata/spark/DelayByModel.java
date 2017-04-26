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

    public void analyze(String inputDir, String outputDir, JavaSparkContext context) {
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

        JavaRDD<String> modelMap =
                onlyDelayed.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(",")[10]));

        JavaPairRDD<String, Integer> frequency =
                modelMap.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts =
                frequency.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) ->
                        integer + integer2);

        JavaPairRDD<String, Integer> upperCounts =
                counts.filter((Tuple2<String, Integer> countPair) -> countPair._2()
                        >= COUNT_TOP_50_LIMIT);

        SparkUtils.saveCoalescedRDDToJsonFile(upperCounts, outputDir + "/model-upper-count");

        List<Tuple2<String, Integer>> output = counts.collect();

        Comparator<Tuple2<String, Integer>> comparator = Comparator.comparing(Tuple2::_2);

        Collections.sort(output, comparator);

        /**
         * Average delays
         */

        JavaPairRDD<String, Tuple2<Long, Long>> combineRDDs =
                onlyDelayed.mapToPair((String line) -> {
                    String model = line.split(",")[10];
                    long delayAmount = Long.parseLong(line.split(",")[15]);
                    long count = 1L;
                    return new Tuple2<>(model, new Tuple2<>(delayAmount, count));
                })
                        .reduceByKey((Tuple2<Long, Long> tuple1, Tuple2<Long, Long> tuple2) -> new Tuple2<>(
                                tuple1._1 + tuple2._1,
                                tuple1._2 + tuple2._2));

        JavaPairRDD<String, Double> answerRDD =
                combineRDDs.mapValues((Tuple2<Long, Long> tuple1) -> new Double(
                        (tuple1._1) / (tuple1._2)));

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

        JavaPairRDD<String, Tuple2<Double, Double>> percentRDD =
                allFlights.mapToPair((String s) -> {
                    String airplane = s.split(",")[10];
                    double delayedFlightCount = 0;
                    double totalFlightCount;
                    if (Integer.parseInt(s.split(",")[15]) > 15) {
                        delayedFlightCount = 1;
                        totalFlightCount = 1;
                    } else {
                        totalFlightCount = 1;
                    }
                    return new Tuple2<>(airplane, new Tuple2<>(delayedFlightCount,
                            totalFlightCount));
                })
                        .reduceByKey((Tuple2<Double, Double> tupleOne, Tuple2<Double, Double> tupleTwo) -> new Tuple2<>(
                                tupleOne._1 + tupleTwo._1,
                                tupleOne._2 + tupleTwo._2));

        JavaPairRDD<String, Double> percentModelDelayRDD =
                percentRDD.mapValues((Tuple2<Double, Double> percentTuple) -> new Double(
                        (percentTuple._1 * 100d) / (percentTuple._2)));

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
        delayByModel.analyze(args[0], args[1], context);

        context.stop();
    }
}
