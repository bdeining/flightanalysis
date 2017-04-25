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
import scala.Tuple3;

public class SparkUtils {

    private static final int DELAY_TIME_MINUTES_INDEX = 15;

    private static final int MONTH_INDEX = 1;

    private static final int DAY_OF_MONTH_INDEX = 2;

    private static final int DAY_OF_WEEK_INDEX = 3;

    private static final int YEAR_INDEX = 0;

    private static final int yearIndex = 0;

    private static final int uniqueCarrierCodeIndex = 8;

    private static final int tailNumIndex = 10;

    private static final int distanceIndex = 18;

    private static final int isCancelledIndex = 21;
    private static final int cancellationCode = 22;

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

    public static JavaRDD<FlightInfo> getFlightInfoRddFromLines(JavaRDD<String> lines) {

        JavaRDD<FlightInfo> flightInfo = lines.map(
                (Function<String, FlightInfo>) s -> {
                    String[] fields = s.split(",");

                    // create flight info obj
                    FlightInfo fci = new FlightInfo();
                    fci.setYear(fields[yearIndex]);
                    fci.setMonth(fields[MONTH_INDEX]);

                    fci.setCancelCode(fields[cancellationCode]);
                    fci.setIsCancelled(fields[isCancelledIndex]);

                    fci.setFlightDelay(fields[DELAY_TIME_MINUTES_INDEX]);

                    fci.setUniqueCarrierCode(fields[uniqueCarrierCodeIndex]);

                    fci.setTailNum(fields[tailNumIndex].trim());

                    fci.setDistance(fields[distanceIndex]);

                    return fci;
                }
        );

        return flightInfo;
    }
}
