package cs455.flightdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * a. most and least delayed airports b. average delay by airport
 */

public class AirportAnalysis {

    //found by sorting output and printing it
    private static int upperCountLimit = 92625;
    private static int lowerCountLimit = 290;
    private static double percentTop50Limit = 18.06;
    private static double percentBottom50Limit = 8.41;

    public void analyze(String inputDir, String outputDir, JavaSparkContext context) {
        JavaRDD<String> lines = context.textFile(inputDir);

        JavaRDD<String> onlyDelayed = lines.filter(
                (Function<String, Boolean>) s -> {
                    try {
                        return (Integer.parseInt(s.split(",")[15])) > 15;
                    } catch (NumberFormatException e) {
                        return false;
                    }
                }
        );

        JavaRDD<String> allFlights = lines.filter(
                (Function<String, Boolean>) s -> {
                    try {
                        Integer.parseInt(s.split(",")[15]);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                    return true;
                }
        );

        /**
         * Counts (a.)
         */

        JavaRDD<String> airportMap = onlyDelayed.flatMap(
                (FlatMapFunction<String, String>) s -> Arrays.asList(s.split(",")[16]));

        JavaPairRDD<String, Integer> frequency = airportMap.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = frequency.reduceByKey(
                (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        JavaPairRDD<String, Integer> filteredUpperCounts =
                counts.filter((Tuple2<String, Integer> upperCountPair) -> upperCountPair._2() >= upperCountLimit);

        JavaPairRDD<String, Integer> filteredLowerCounts =
                counts.filter((Tuple2<String, Integer> lowerCountPair) -> lowerCountPair._2() <= lowerCountLimit);

        SparkUtils.saveCoalescedRDDToJsonFile(filteredUpperCounts, outputDir + "/airport-upper-count");
        SparkUtils.saveCoalescedRDDToJsonFile(filteredLowerCounts, outputDir + "/airport-lower-count");

        List<Tuple2<String, Integer>> output = counts.collect();
        Comparator<Tuple2<String, Integer>> comparator = Comparator.comparing(Tuple2::_2);
        Collections.sort(output, comparator);

        /**
         * Average delays (b.)
         */

        JavaPairRDD<String, Tuple2<Long, Long>> combineRDDs =
                onlyDelayed.mapToPair((String line) -> {
                    String airport = line.split(",")[16];
                    long delayAmount = Long.parseLong(line.split(",")[15]);
                    long count = 1L;
                    return new Tuple2<>(airport, new Tuple2<>(delayAmount, count));
                }).reduceByKey((Tuple2<Long, Long> tuple1, Tuple2<Long, Long> tuple2)
                        -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

        JavaPairRDD<String, Double> answerRDD =
                combineRDDs.mapValues((Tuple2<Long, Long> tuple1)
                        -> new Double(tuple1._1 / (tuple1._2)));

        SparkUtils.saveCoalescedRDDToJsonFile(answerRDD, outputDir + "/airport-average");

        List<Tuple2<String, Double>> answer = answerRDD.collect();

        /**
         * Percent delays
         */

        JavaPairRDD<String, Tuple2<Double, Double>> percentRDD =
                allFlights.mapToPair((String s) -> {
                    String originAirport = s.split(",")[16];
                    double delayedFlightCount = 0;
                    double totalFlightCount;
                    if (Integer.parseInt(s.split(",")[15]) > 15) {
                        delayedFlightCount = 1;
                        totalFlightCount = 1;
                    } else {
                        totalFlightCount = 1;
                    }
                    return new Tuple2<>(originAirport, new Tuple2<>(delayedFlightCount, totalFlightCount));
                }).reduceByKey((Tuple2<Double, Double> tupleOne, Tuple2<Double, Double> tupleTwo)
                        -> new Tuple2<>(tupleOne._1 + tupleTwo._1, tupleOne._2 + tupleTwo._2));

        JavaPairRDD<String, Double> percentAirportDelayRDD =
                percentRDD.mapValues((Tuple2<Double, Double> percentTuple)
                        -> new Double((percentTuple._1 * 100d) / (percentTuple._2)));

        JavaPairRDD<String, Double> filteredUpperPercentDelay =
                percentAirportDelayRDD.filter((Tuple2<String, Double> topPairs) -> topPairs._2() >= percentTop50Limit);

        JavaPairRDD<String, Double> filteredLowerPercentageRDD =
                percentAirportDelayRDD.filter((Tuple2<String, Double> bottomPairs) -> bottomPairs._2() <= percentBottom50Limit);

        SparkUtils.saveCoalescedRDDToJsonFile(filteredUpperPercentDelay, outputDir + "/airport-upper-percent");
        SparkUtils.saveCoalescedRDDToJsonFile(filteredLowerPercentageRDD, outputDir + "/airport-lower-percent");

        JavaPairRDD<Double, String> swappedRDD =
                percentAirportDelayRDD.mapToPair(Tuple2::swap);

        swappedRDD.sortByKey(); //can use later so output file's sorted

        List<Tuple2<String, Double>> percentList = percentAirportDelayRDD.collect();

        Comparator<Tuple2<String, Double>> percentComparator = Comparator.comparing(Tuple2::_2);

        Collections.sort(percentList, percentComparator);


        /**
         * Print answers
         */
        System.out.println("*** A ***");
        System.out.println("Most:");
        for (int i = output.size() - 1; i > output.size() - 50; i--) {
            System.out.println(output.get(i)._1() + ":" + output.get(i)._2());
        }
        System.out.println("Least:");
        for (int i = 0; i < 50; i++) {
            System.out.println(output.get(i)._1() + ":" + output.get(i)._2());
        }
        System.out.println();
        for (Tuple2<String, Integer> pair : output) {
            System.out.println(pair._1 + ":" + pair._2);
        }

        System.out.println("*** B, average delays by airport ***");
        for (int i = 0; i < 50; i++) {
            System.out.println(answer.get(i)._1() + ":" + answer.get(i)._2());
        }

        System.out.println("*** C, Percent delays by airport ***");
        System.out.println("Most:");
        for (int i = output.size() - 1; i > output.size() - 50; i--) {
            System.out.println(percentList.get(i)._1() + ":" + percentList.get(i)._2());
        }
        System.out.println("Least:");
        for (int i = 0; i < 50; i++) {
            System.out.println(percentList.get(i)._1() + ":" + percentList.get(i)._2());
        }
    }

    public static void main(String[] args) {
        AirportAnalysis airportAnalysis = new AirportAnalysis();
        SparkConf sparkConf = new SparkConf().setAppName("tp-airport-analysis");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        airportAnalysis.analyze(args[0], args[1], context);
        context.stop();
    }
}
