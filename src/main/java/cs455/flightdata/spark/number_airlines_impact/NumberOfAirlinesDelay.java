package cs455.flightdata.spark.number_airlines_impact;

import cs455.flightdata.spark.FlightAnalysisIface;
import cs455.flightdata.spark.FlightInfo;
import cs455.flightdata.spark.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;

/**
 * Created by scrhoads on 4/23/17.
 */
public class NumberOfAirlinesDelay implements FlightAnalysisIface {

    @Override
    public boolean executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> allFlightsInfo, String outputDir) {

        //
        // get distinct number of airlines over time
        //

        JavaPairRDD<Tuple2<String, String>, String> airlinesByMonth = allFlightsInfo.mapToPair(
                (PairFunction<FlightInfo, Tuple2<String, String>, String>) fci ->
                        new Tuple2<>(new Tuple2<>(fci.getYear(), fci.getMonth()), fci.getUniqueCarrierCode())
        );

        // distinct should give us only one occurance of unique carrier code for each year/month tuple
        JavaPairRDD<Tuple2<String, String>, String> distinctAirlinesByMonth = airlinesByMonth.distinct();

        // get counts for number of airlines that flew each month
        // first need to get setup for reduction
        JavaPairRDD<Tuple2<String, String>, Long> countsForEachMonth = distinctAirlinesByMonth.mapToPair(
                (PairFunction<Tuple2<Tuple2<String, String>, String>, Tuple2<String, String>, Long>) tup ->
                        new Tuple2<>(tup._1, new Long(1))
        );

        // count them up
        JavaPairRDD<Tuple2<String, String>, Long> totalsForEachMonth = countsForEachMonth.reduceByKey(
                (l1, l2) -> l1 + l2
        );

        // save
        SparkUtils.saveCoalescedRDDToJsonFile(totalsForEachMonth,
                outputDir + File.separator + "number_airlines_each_month");

        //
        // get number of flights delayed by year/month
        //

        JavaRDD<FlightInfo> occurancesOfDelays = allFlightsInfo.filter(
                (Function<FlightInfo, Boolean>) fci -> Long.parseLong(fci.getFlightDelay()) > 15
        );

        JavaPairRDD<Tuple2<String, String>, Long> yearMonthDelayOccurances = occurancesOfDelays.mapToPair(
                (PairFunction<FlightInfo, Tuple2<String, String>, Long>) fci ->
                        new Tuple2<>(new Tuple2(fci.getYear(), fci.getMonth()), new Long(1))
        );

        // count occurances by key
        JavaPairRDD<Tuple2<String, String>, Long> yearMonthDelaySum = yearMonthDelayOccurances.reduceByKey(
                (l1, l2) -> l1 + l2
        );

        // save
        SparkUtils.saveCoalescedRDDToJsonFile(yearMonthDelaySum,
                outputDir + File.separator + "number_of_delays_each_month-year");




        return false;
    }
}
