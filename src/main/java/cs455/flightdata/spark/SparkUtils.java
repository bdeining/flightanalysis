package cs455.flightdata.spark;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.gson.Gson;

import scala.Tuple2;
import scala.Tuple3;

public class SparkUtils {

    public static void saveCoalescedTupleRDDToCsvFile(
            JavaPairRDD<String, Tuple2<Long, Long>> javaPairRDD, String outputDirectory) {
        JavaRDD<String> stringJavaRDD = javaPairRDD.map(stringTuple2Tuple2 ->
                stringTuple2Tuple2._1() + "," + stringTuple2Tuple2._2()
                        ._1() + "," + stringTuple2Tuple2._2()
                        ._2());
        stringJavaRDD.coalesce(1)
                .saveAsTextFile(outputDirectory);
    }

    public static void saveCoalescedLongRDDToCsvFile(JavaPairRDD<String, Long> javaPairRDD,
            String outputDirectory) {
        JavaRDD<String> stringJavaRDD = javaPairRDD.map(stringTuple2Tuple2 ->
                stringTuple2Tuple2._1() + "," + stringTuple2Tuple2._2());
        stringJavaRDD.coalesce(1)
                .saveAsTextFile(outputDirectory);
    }

    public static void saveCoalescedRDDToJsonFile(JavaPairRDD<?, ?> javaPairRDD,
            String outputDirectory) {
        try (FileSystem fs = FileSystem.get(javaPairRDD.context().hadoopConfiguration());
             FSDataOutputStream outputStream = fs.create(new Path(outputDirectory + "/results.json"))) {

            Gson gson = new Gson();
            outputStream.writeChars(gson.toJson(javaPairRDD.coalesce(1)
                    .collect()));
            outputStream.writeChars("\n");
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveCoalescedTuple3LongToCsvFile(
            JavaPairRDD<Tuple3<String, String, String>, Long> tuple3LongRdd, String outputDirectory) {

        JavaRDD<String> stringJavaRDD = tuple3LongRdd.map(stringTuple3Tuple ->
            stringTuple3Tuple._1()._1() + "," + stringTuple3Tuple._1()._2() + "," + stringTuple3Tuple._1()._3() + ","
                    + stringTuple3Tuple._2()
        );

        stringJavaRDD.coalesce(1).saveAsTextFile(outputDirectory);

    }
}
