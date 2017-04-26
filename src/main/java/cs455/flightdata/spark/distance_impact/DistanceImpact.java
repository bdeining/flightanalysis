package cs455.flightdata.spark.distance_impact;

import java.io.File;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import cs455.flightdata.spark.FlightInfo;
import cs455.flightdata.spark.SparkUtils;
import scala.Tuple2;

public class DistanceImpact implements Serializable {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: DistanceImpact <input-directory> <output-directory>");
            System.exit(1);
        }

        String outputDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("DistanceImpact");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<FlightInfo> flightInfo = SparkUtils.getFlightInfoRddFromLines(lines);

        DistanceImpact distanceImpact = new DistanceImpact();
        distanceImpact.executeAnalysis(ctx, flightInfo, outputDir);

        ctx.stop();
    }

    public void executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> allFlights,
            String outputDir) {

        //
        // look at distance groupings of 500 mi (e.g. 0-500, 501-1000 .... >5000
        //

        // get all flights grouping
        JavaPairRDD<String, Long> allFlightsDistanceGroupRdd =
                allFlights.mapToPair((PairFunction<FlightInfo, String, Long>) fci -> new Tuple2<>(
                                distanceGroupingName(fci.getDistance()),
                                new Long(1)))
                        .reduceByKey((l1, l2) -> l1 + l2);

        // find cancelled flights
        JavaPairRDD<String, Long> onlyCancelledGroups =
                allFlights.filter((Function<FlightInfo, Boolean>) s -> s.getIsCancelled() == 1)
                        .mapToPair((PairFunction<FlightInfo, String, Long>) fci -> new Tuple2<>(
                                        distanceGroupingName(fci.getDistance()),
                                        new Long(1)))
                        .reduceByKey((l1, l2) -> l1 + l2);

        // do the same but for delayed flights
        JavaPairRDD<String, Long> onlyDelayedFlights =
                allFlights.filter((Function<FlightInfo, Boolean>) fci -> FlightInfo.hasDelay(fci.getFlightDelay()))
                        .mapToPair((PairFunction<FlightInfo, String, Long>) fci -> new Tuple2<>(
                                        distanceGroupingName(fci.getDistance()),
                                        new Long(1)))
                        .reduceByKey((l1, l2) -> l1 + l2);

        // join this data with existing all flight and cancelled
        JavaPairRDD<String, Iterable<Long>> allCancelledDelayedDistGroups = ctx.union(
                allFlightsDistanceGroupRdd,
                onlyCancelledGroups,
                onlyDelayedFlights)
                .groupByKey();

        // save this data set out
        SparkUtils.saveCoalescedRDDToJsonFile(allCancelledDelayedDistGroups,
                outputDir + File.separator + "distance_impact");

    }

    private String distanceGroupingName(String distanceStr) {

        String distanceGroupName = "NA";

        int distance = -1;

        if (!distanceStr.equals("NA")) {

            try {
                distance = Integer.parseInt(distanceStr);
            } catch (NumberFormatException nfe) {
                System.err.println("error parsing distanceStr: " + distanceStr);
            }

            if (distance >= 0 && distance <= 500) {
                distanceGroupName = "0-500";
            } else if (distance > 500 && distance <= 1000) {
                distanceGroupName = "501-1000";
            } else if (distance > 1000 && distance <= 1500) {
                distanceGroupName = "1001-1500";
            } else if (distance > 1500 && distance <= 2000) {
                distanceGroupName = "1501-2000";
            } else if (distance > 2000 && distance <= 2500) {
                distanceGroupName = "2001-2500";
            } else if (distance > 2500 && distance <= 3000) {
                distanceGroupName = "2501-3000";
            } else if (distance > 3000 && distance <= 3500) {
                distanceGroupName = "3001-3500";
            } else if (distance > 3500 && distance <= 4000) {
                distanceGroupName = "3501-4000";
            } else if (distance > 4000 && distance <= 4500) {
                distanceGroupName = "4001-4500";
            } else if (distance > 4500 && distance <= 5000) {
                distanceGroupName = "4501-5000";
            } else if (distance > 5000) {
                distanceGroupName = "> 5000";
            }
        }

        return distanceGroupName;
    }
}
