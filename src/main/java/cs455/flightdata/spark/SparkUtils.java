package cs455.flightdata.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class SparkUtils {

    public static void saveCoalescedTupleRDDToCsvFile(JavaPairRDD<String, Tuple2<Long, Long>> javaPairRDD, String outputDirectory) {
        JavaRDD<String> stringJavaRDD = javaPairRDD.map(stringTuple2Tuple2 -> stringTuple2Tuple2._1() + "," + stringTuple2Tuple2._2()._1() + "," + stringTuple2Tuple2._2()._2());
        stringJavaRDD.coalesce(1).saveAsTextFile(outputDirectory);
    }

    public static void saveCoalescedLongRDDToCsvFile(JavaPairRDD<String, Long> javaPairRDD, String outputDirectory) {
        JavaRDD<String> stringJavaRDD = javaPairRDD.map(stringTuple2Tuple2 -> stringTuple2Tuple2._1() + "," + stringTuple2Tuple2._2());
        stringJavaRDD.coalesce(1).saveAsTextFile(outputDirectory);
    }
}
