package cs455.flightdata.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class DSTAnalysis {

    public void analyze(String inputDir, String outputDir, JavaSparkContext context) {
        JavaRDD<String> lines = context.textFile(inputDir);

        JavaRDD<String> onlyDST = lines.filter((Function<String, Boolean>) s -> {
                    if (s.contains("1987,4,5") ||
                            s.contains("1987,10,25") ||
                            s.contains("1988,4,3") ||
                            s.contains("1988,10,30") ||
                            s.contains("1989,4,2") ||
                            s.contains("1989,10,29") ||
                            s.contains("1990,4,1") ||
                            s.contains("1990,10,28") ||
                            s.contains("1991,4,7") ||
                            s.contains("1991,10,27") ||
                            s.contains("1992,4,5") ||
                            s.contains("1992,10,25") ||
                            s.contains("1993,4,4") ||
                            s.contains("1993,10,31") ||
                            s.contains("1994,4,3") ||
                            s.contains("1994,10,30") ||
                            s.contains("1995,4,2") ||
                            s.contains("1995,10,29") ||
                            s.contains("1996,4,7") ||
                            s.contains("1996,10,27") ||
                            s.contains("1997,4,6") ||
                            s.contains("1997,10,26") ||
                            s.contains("1998,4,5") ||
                            s.contains("1998,10,25") ||
                            s.contains("1999,4,4") ||
                            s.contains("1999,10,31") ||
                            s.contains("2000,4,2") ||
                            s.contains("2000,10,29") ||
                            s.contains("2001,4,1") ||
                            s.contains("2001,10,28") ||
                            s.contains("2002,4,7") ||
                            s.contains("2002,10,27") ||
                            s.contains("2003,4,6") ||
                            s.contains("2003,10,26") ||
                            s.contains("2004,4,4") ||
                            s.contains("2004,10,31") ||
                            s.contains("2005,4,3") ||
                            s.contains("2005,10,30") ||
                            s.contains("2006,4,2") ||
                            s.contains("2006,10,29") ||
                            s.contains("2007,3,11") ||
                            s.contains("2007,11,4") ||
                            s.contains("2008,3,9") ||
                            s.contains("2008,11,2")) {
                        try {
                            Integer.parseInt(s.split(",")[15]); //ignore result, just remove "NA" lines
                            //and include all flights
                        } catch (NumberFormatException e) {
                            return false;
                        }
                        return true;
                    } else {
                        return false;
                    }
                });

        /**
         * a. Average delay amount on DST start/end per year
         */

        JavaRDD<String> delayedDST = onlyDST.filter((Function<String, Boolean>) delays ->
                Integer.parseInt(delays.split(",")[15]) > 15);

        JavaPairRDD<String, Tuple2<Long, Long>> combineRDDs =
                delayedDST.mapToPair((String line) -> {
                    String dst = line.split(",")[0];
                    long delayAmount = Long.parseLong(line.split(",")[15]);
                    long count = 1L;
                    return new Tuple2<>(dst, new Tuple2<>(delayAmount, count));
                })
                        .reduceByKey((Tuple2<Long, Long> tuple1, Tuple2<Long, Long> tuple2) -> new Tuple2<>(
                                tuple1._1 + tuple2._1,
                                tuple1._2 + tuple2._2));

        JavaPairRDD<String, Double> answerRDD =
                combineRDDs.mapValues((Tuple2<Long, Long> tuple1) -> new Double(
                        tuple1._1 / (tuple1._2)));

        SparkUtils.saveCoalescedRDDToJsonFile(answerRDD, outputDir + "/dst-average");
        List<Tuple2<String, Double>> answer = answerRDD.collect();

        /**
         * b. Percentage of DST flights delayed per year
         */

        JavaPairRDD<String, Tuple2<Double, Double>> percentRDD = onlyDST.mapToPair((String s) -> {
            String allDST = s.split(",")[0];
            double delayedFlightCount = 0;
            double totalFlightCount;
            if (Integer.parseInt(s.split(",")[15]) > 15) {
                delayedFlightCount = 1;
                totalFlightCount = 1;
            } else {
                totalFlightCount = 1;
            }
            return new Tuple2<>(allDST, new Tuple2<>(delayedFlightCount, totalFlightCount));
        })
                .reduceByKey((Tuple2<Double, Double> tupleOne, Tuple2<Double, Double> tupleTwo) -> new Tuple2<>(
                        tupleOne._1 + tupleTwo._1,
                        tupleOne._2 + tupleTwo._2));

        JavaPairRDD<String, Double> percentDSTDelay =
                percentRDD.mapValues((Tuple2<Double, Double> percentTuple) -> new Double(
                        (percentTuple._1 * 100d) / (percentTuple._2)));

        SparkUtils.saveCoalescedRDDToJsonFile(percentDSTDelay, outputDir + "/dst-percent");
        List<Tuple2<String, Double>> percentList = percentDSTDelay.collect();

        /**
         * Print answers
         */
        System.out.println("*** Averages ***");
        for (Tuple2<String, Double> averagePair : answer) {
            System.out.println(averagePair._1 + ":" + averagePair._2);
        }

        System.out.println("*** Percentages ***");
        for (Tuple2<String, Double> tuple : percentList) {
            System.out.println(tuple._1 + ":" + tuple._2);
        }
    }

    public static void main(String[] args) {
        DSTAnalysis dstAnalysis = new DSTAnalysis();
        SparkConf sparkConf = new SparkConf().setAppName("tp-dst-analysis");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        dstAnalysis.analyze(args[0], args[1], context);
        context.stop();
    }
}
