package cs455.flightdata.spark;

import org.apache.spark.api.java.JavaPairRDD;

public class SparkUtils {

    public static void saveCoalescedRDDToTextFile(JavaPairRDD<?, ?> javaPairRDD, String outputDirectory) {
        javaPairRDD.coalesce(1).saveAsTextFile(outputDirectory);
    }

}
