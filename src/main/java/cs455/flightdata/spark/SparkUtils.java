package cs455.flightdata.spark;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class SparkUtils {

    public static void saveCoalescedRDDToTextFile(JavaPairRDD<?, ?> javaPairRDD, String outputDirectory) {
        javaPairRDD.coalesce(1).saveAsTextFile(outputDirectory);
    }

    public static void saveCoalescedRDDToJsonFile(JavaPairRDD<?, ?> javaPairRDD, String outputDirectory) {
        try
        {
            // get the filesystem from the RDD's configuration

            FileSystem fs = FileSystem.get(javaPairRDD.context().hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(outputDirectory + "/results.json"));
            Gson gson = new Gson();

            outputStream.writeChars(gson.toJson(javaPairRDD.coalesce(1).collect()));
            outputStream.writeChars("\n");
            outputStream.flush();
            outputStream.close();
            fs.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
