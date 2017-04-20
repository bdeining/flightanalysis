package cs455.flightdata.spark;

import org.apache.spark.api.java.JavaPairRDD;

public class SparkUtils {

    public static void saveRDDToTextFile(JavaPairRDD<?, ?> javaPairRDD, String outputDirectory) {
        javaPairRDD.saveAsTextFile(outputDirectory);
    }

}
