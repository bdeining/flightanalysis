package cs455.flightdata.spark;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.gson.Gson;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class SparkUtils {

    private static final int DELAY_TIME_MINUTES_INDEX = 15;

    private static final int MONTH_INDEX = 1;

    private static final int YEAR_INDEX = 0;

    private static final int UNIQUE_CARRIER_CODE_INDEX = 8;

    private static final int TAIL_NUM_INDEX = 10;

    private static final int DISTANCE_INDEX = 18;

    private static final int IS_CANCELLED_INDEX = 21;

    private static final int CANCELLATION_CODE = 22;

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

    public static JavaRDD<FlightInfo> getFlightInfoRddFromLines(JavaRDD<String> lines) {

        JavaRDD<FlightInfo> flightInfo = lines.map(
                (Function<String, FlightInfo>) s -> {
                    String[] fields = s.split(",");

                    // create flight info obj
                    FlightInfo fci = new FlightInfo();
                    fci.setYear(fields[YEAR_INDEX]);
                    fci.setMonth(fields[MONTH_INDEX]);

                    fci.setCancelCode(fields[CANCELLATION_CODE]);
                    fci.setIsCancelled(fields[IS_CANCELLED_INDEX]);

                    fci.setFlightDelay(fields[DELAY_TIME_MINUTES_INDEX]);

                    fci.setUniqueCarrierCode(fields[UNIQUE_CARRIER_CODE_INDEX]);

                    fci.setTailNum(fields[TAIL_NUM_INDEX].trim());

                    fci.setDistance(fields[DISTANCE_INDEX]);

                    return fci;
                }
        );

        return flightInfo;
    }
}
