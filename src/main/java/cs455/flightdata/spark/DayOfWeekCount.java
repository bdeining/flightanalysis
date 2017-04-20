package cs455.flightdata.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DayOfWeekCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("tp-dow-count");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = context.textFile(args[0]);

        JavaRDD<String> onlyDelays = lines.filter((string) -> (
                Integer.parseInt(string.split(",")[14]) + Integer.parseInt(string.split(",")[15])
                        > 0));

        JavaRDD<String> daysOfWeekMap = onlyDelays.flatMap((string) -> Arrays.asList(string.split(
                ",")[3]));

        JavaPairRDD<String, Integer> frequency = daysOfWeekMap.mapToPair((string) -> new Tuple2<>(
                string,
                1));

        JavaPairRDD<String, Integer> counts = frequency.reduceByKey((integer1, integer2) -> (
                integer1 + integer2));

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<String, Integer> tuple2 : output) {
            System.out.println(tuple2._1() + ":" + tuple2._2());
        }
        context.stop();
    }
}