package cs455.flightdata.spark.flight_cancellation;

import cs455.flightdata.spark.FlightInfo;
import cs455.flightdata.spark.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Outputs csvs that look at the counts for flight cancellation broken down by:
 * all time, year, month, day, year-month, year-month-day
 *
 * relevant indexes are: 21 - isCancelled == 1, 22 - cancellation code: A-Carrier, B-Weather, C-National Air System
 */
public class FlightCancellation implements Serializable {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: FlightCancellation <input-directory> <output-directory>");
            System.exit(1);
        }

        String outputDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("FlightCancellationAnalysis");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<FlightInfo> flightInfo = SparkUtils.getFlightInfoRddFromLines(lines);

        FlightCancellation flightCancellation = new FlightCancellation();
        flightCancellation.executeAnalysis(ctx, flightInfo, outputDir);

        ctx.stop();
    }

    public boolean executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> allFlights, String outputDir) {

        // create rdd w/ list of words
//        JavaRDD<List<String>> linesOfLists = lines.map(
//                (Function<String, List<String>>) s -> Arrays.asList(s.split(","))
//        );

        // create total number of flights for entire dataset
        Long totalNumFlights = allFlights.count();

        List<Tuple2<String, Long>> tupList = new ArrayList();
        tupList.add(new Tuple2<>("All Flights", totalNumFlights));

        JavaPairRDD<String, Long> allFlightCount = ctx.parallelizePairs(tupList);

        // filter down to flights that have been cancelled
        JavaRDD<FlightInfo> onlyCancelled = allFlights.filter(
                (Function<FlightInfo, Boolean>) s -> s.getIsCancelled() == 1
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

        JavaPairRDD<String, Long> codeSumsAndAllFlightCoutns = codeSums.union(allFlightCount);

        SparkUtils.saveCoalescedRDDToJsonFile(codeSumsAndAllFlightCoutns,
                outputDir + File.separator + "aggregate_flight_cancellation_counts");

        //
        // cancellations by month/year - still at aggregate level
        //

        // create pair rdd w/ year, month, and cancellation code as key and 1 as value
        JavaPairRDD<Tuple3<String, String, String>, Long> cancellationsByYearMonthCode = onlyCancelled.mapToPair(
                (PairFunction<FlightInfo, Tuple3<String, String, String>, Long>) s ->
                        new Tuple2<>(new Tuple3<>(s.getYear(), s.getMonth(), s.getCancelCodeHumanReadableString()), new Long(1))

        );

        // count up reasons
        JavaPairRDD<Tuple3<String, String, String>, Long> cancellationsByYearMonthCodeReduced =
                cancellationsByYearMonthCode.reduceByKey(
                        (l1, l2) -> l1 + l2
                );

        SparkUtils.saveCoalescedRDDToJsonFile(cancellationsByYearMonthCodeReduced,
                outputDir + File.separator + "monthly_cancellation_counts_by_code");

        //
        // cancellations by airline
        //

        // get all flights by carrier code
        JavaPairRDD<String, Long> carrierCodeFreqInAllFlights = allFlights.mapToPair(
                (PairFunction<FlightInfo, String, Long>) fci ->
                        new Tuple2<>(fci.getUniqueCarrierCode(), new Long(1))
        ).reduceByKey(
                (l1, l2) -> l1 + l2
        );

        // do word count on carrier code
        JavaPairRDD<String, Long> carrierCodeFreqInCancelled = onlyCancelled.mapToPair(
                (PairFunction<FlightInfo, String, Long>) fci -> new Tuple2<>(fci.getUniqueCarrierCode(), new Long(1))
        );

        // count them up
        JavaPairRDD<String, Long> carrierCodeCancellationSums = carrierCodeFreqInCancelled.reduceByKey(
                (l1, l2) -> l1 + l2
        );

        JavaPairRDD<String, Iterable<Long>> carrierCodeAllAndCancelledFlights =
                carrierCodeFreqInAllFlights.union(carrierCodeCancellationSums).groupByKey();

        // save to csv
        SparkUtils.saveCoalescedRDDToJsonFile(carrierCodeAllAndCancelledFlights,
                outputDir + File.separator + "unique-carrier-code_cancellation_counts");



        return false;
    }

}
