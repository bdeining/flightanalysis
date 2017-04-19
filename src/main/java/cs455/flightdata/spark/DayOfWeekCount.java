package cs455.tp.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DayOfWeekCount {

    public static void main(String[] args) {
	SparkConf sparkConf = new SparkConf().setAppName("tp-dow-count");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = context.textFile(args[0]);

        JavaRDD<String> onlyDelays = lines.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        return (Integer.parseInt(s.split(",")[14]) +
                                Integer.parseInt(s.split(",")[15])) > 0;
                    }
                }
        );

        JavaRDD<String> daysOfWeekMap = onlyDelays.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(",")[3]);
                    }
                });

        JavaPairRDD<String, Integer> frequency = daysOfWeekMap.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = frequency.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<String, Integer> tuple2 : output) {
            System.out.println(tuple2._1() + ":" + tuple2._2());
        }
	context.stop();
    }
}


//            .flatMap(s -> Arrays.asList(s.split(",")).iterator())
//                    .mapToPair(word -> new Tuple2<>(word, 1))
//        .reduceByKey((a, b) -> a + b);

//            .filter(s -> Integer.parseInt(s.split(",")[14]) - Integer.parseInt(s.split(",")[15]) > 0)
//                    .filter(l -> l.split(",")[3])
//                    .flatMap(m -> Arrays.asList(m.split(",")).iterator())
//                    .mapToPair(word -> new Tuple2<>(word, 1))
//        .reduceByKey((a, b) -> a + b);
