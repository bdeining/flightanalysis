package cs455.flightdata.spark.flight_cancellation;

import cs455.flightdata.spark.FlightAnalysisIface;
import cs455.flightdata.spark.FlightInfo;
import cs455.flightdata.spark.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;

/**
 * Created by scrhoads on 4/19/17.
 *
 * Outputs csvs that look at the counts for flight cancellation broken down by:
 * all time, year, month, day, year-month, year-month-day
 *
 * relevant indexes are: 21 - isCancelled == 1, 22 - cancellation code: A-Carrier, B-Weather, C-National Air System
 */
public class FlightCancellation implements FlightAnalysisIface, Serializable {



    @Override
    public boolean executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> reasonsForCancellation, String outputDir) {

        // create rdd w/ list of words
//        JavaRDD<List<String>> linesOfLists = lines.map(
//                (Function<String, List<String>>) s -> Arrays.asList(s.split(","))
//        );



        // filter down to flights that have been delayed
        JavaRDD<FlightInfo> onlyCancelled = reasonsForCancellation.filter(
                (Function<FlightInfo, Boolean>) s -> s.getIsCancelled().equals("1")
        );

        // create pairs of code,1
        JavaPairRDD<String, Long> codeFrequency = onlyCancelled.mapToPair(
                (PairFunction<FlightInfo, String, Long>) s -> new Tuple2<>(s.getCancelCodeHumanReadableString(),
                        new Long(1))
        );

//        SparkUtils.saveCoalescedLongRDDToCsvFile(codeFrequency,
//                outputDir + File.separator + "flight_cancellations.csv");

        // count them up
        JavaPairRDD<String, Long> codeSums = codeFrequency.reduceByKey(
                (l1, l2) -> l1 + l2
        );

        SparkUtils.saveCoalescedLongRDDToCsvFile(codeSums,
                outputDir + File.separator + "aggregate_flight_cancellation_counts.csv");

        //
        // cancellations by month/year - still at aggregate level
        //

        // create pair rdd w/ year, month, and cancellation code as key and 1 as value
        JavaPairRDD<Tuple3<String, String, String>, Long> cancellationsByYearMonthCode = onlyCancelled.mapToPair(
                (PairFunction<FlightInfo, Tuple3<String, String, String>, Long>) s ->
                        new Tuple2<>(new Tuple3<>(s.getYear(), s.getMonth(), s.getCancelCodeHumanReadableString()), new Long(1))

        );

        SparkUtils.saveCoalescedTuple3LongToCsvFile(cancellationsByYearMonthCode,
                outputDir + File.separator + "monthly_cancellation_counts");

        //
        // cancellations by airline
        //

        // do word count on carrier code
        JavaPairRDD<String, Long> carrierCodeFreqInCancelled = onlyCancelled.mapToPair(
                (PairFunction<FlightInfo, String, Long>) fci -> new Tuple2<>(fci.getUniqueCarrierCode(), new Long(1))
        );

        // count them up
        JavaPairRDD<String, Long> carrierCodeCancellationSums = carrierCodeFreqInCancelled.reduceByKey(
                (l1, l2) -> l1 + l2
        );

        // save to csv
        SparkUtils.saveCoalescedLongRDDToCsvFile(carrierCodeCancellationSums,
                outputDir + File.separator + "unique-carrier-code_cancellation_counts");



        return false;
    }

}
