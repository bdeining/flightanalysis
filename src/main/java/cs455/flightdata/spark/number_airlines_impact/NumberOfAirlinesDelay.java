package cs455.flightdata.spark.number_airlines_impact;

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

public class NumberOfAirlinesDelay implements Serializable {

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println(
                    "Usage: FlightCancellation <input-directory> <output-directory> <tail-num-master_file> "
                            + "<manufacturer-model-lookup_file>");
            System.exit(1);
        }

        String outputDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("NumberDelayAnalysis");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<FlightInfo> flightInfo = SparkUtils.getFlightInfoRddFromLines(lines);

        NumberOfAirlinesDelay noad = new NumberOfAirlinesDelay();

        JavaPairRDD<String, String> tailNumToModelRdd = noad.createTailNumMappingRdd(ctx,
                args[2],
                args[3]);

        noad.executeAnalysis(ctx, flightInfo, outputDir, tailNumToModelRdd);

        ctx.stop();
    }

    public JavaPairRDD<String, String> createTailNumMappingRdd(JavaSparkContext ctx,
            String tailNumMasterFile, String manufacturerModelLookupFile) {

        JavaRDD<String> tailNumRdd = ctx.textFile(tailNumMasterFile, 1);
        JavaRDD<String> mfrModelLookupRdd = ctx.textFile(manufacturerModelLookupFile, 1);

        // create rdd that's just tailnum and lookup field
        JavaPairRDD<String, String> tailNumMfrModelCodeRdd =
                tailNumRdd.mapToPair((PairFunction<String, String, String>) fullLine -> {
                            String nNumber = fullLine.substring(0, 4);
                            String mfrModelCode = fullLine.substring(37, 43);

                            return new Tuple2<>(mfrModelCode, nNumber);
                        });

        // create rdd from actref that maps mfrModelCode to Mfr and model strings
        JavaPairRDD<String, String> codeToModelStringsRdd =
                mfrModelLookupRdd.mapToPair((PairFunction<String, String, String>) fullLine -> {
                            String mfrModelCode = fullLine.substring(0, 6);
                            //                    String mfrString = fullLine.substring(8, 37).trim();
                            String modelString = fullLine.substring(39, 58)
                                    .trim();

                            return new Tuple2<>(mfrModelCode, modelString);
                        });

        // join
        JavaPairRDD<String, Tuple2<String, String>> codeToMfrModelTailRdd =
                codeToModelStringsRdd.join(tailNumMfrModelCodeRdd);

        // tailnum to mfrModelCode
        JavaPairRDD<String, String> tailNumToMfr =
                codeToMfrModelTailRdd.mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) valTup -> new Tuple2<>(
                                valTup._2()
                                        ._2(),
                                valTup._2()
                                        ._1()));

        return tailNumToMfr;

    }

    public boolean executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> allFlightsInfo,
            String outputDir, JavaPairRDD<String, String> tailNumToModelRdd) {

        //
        // get distinct number of airlines over time
        //

        JavaPairRDD<Tuple2<String, String>, String> airlinesByMonth =
                allFlightsInfo.mapToPair((PairFunction<FlightInfo, Tuple2<String, String>, String>) fci -> new Tuple2<>(
                                new Tuple2<>(fci.getYear(), fci.getMonth()),
                                fci.getUniqueCarrierCode()));

        // get number of flights in each month
        JavaPairRDD<Tuple2<String, String>, Long> numberFlightsEachMonth =
                airlinesByMonth.mapToPair((PairFunction<Tuple2<Tuple2<String, String>, String>, Tuple2<String, String>, Long>) tup -> new Tuple2<>(
                                tup._1(),
                                new Long(1)))
                        .reduceByKey((l1, l2) -> l1 + l2);

        // distinct should give us only one occurance of unique carrier code for each year/month tuple
        JavaPairRDD<Tuple2<String, String>, String> distinctAirlinesByMonth =
                airlinesByMonth.distinct();

        // get counts for number of airlines that flew each month
        // first need to get setup for reduction
        JavaPairRDD<Tuple2<String, String>, Long> countsForEachMonth =
                distinctAirlinesByMonth.mapToPair((PairFunction<Tuple2<Tuple2<String, String>, String>, Tuple2<String, String>, Long>) tup -> new Tuple2<>(
                                tup._1,
                                new Long(1)));

        // count them up
        JavaPairRDD<Tuple2<String, String>, Long> totalsForEachMonth =
                countsForEachMonth.reduceByKey((l1, l2) -> l1 + l2);

        // save
        //        SparkUtils.saveCoalescedRDDToJsonFile(totalsForEachMonth,
        //                outputDir + File.separator + "number_airlines_each_month-year");

        //
        // get number of flights delayed by year/month
        //

        JavaRDD<FlightInfo> occurancesOfDelays =
                allFlightsInfo.filter((Function<FlightInfo, Boolean>) fci -> !fci.getFlightDelay()
                                .equals("NA") && Long.parseLong(fci.getFlightDelay()) > 15);

        JavaPairRDD<Tuple2<String, String>, Long> yearMonthDelayOccurances =
                occurancesOfDelays.mapToPair((PairFunction<FlightInfo, Tuple2<String, String>, Long>) fci -> new Tuple2<>(
                                new Tuple2(fci.getYear(), fci.getMonth()),
                                new Long(1)));

        // count occurances by key
        JavaPairRDD<Tuple2<String, String>, Long> yearMonthDelaySum =
                yearMonthDelayOccurances.reduceByKey((l1, l2) -> l1 + l2);

        // save
        //        SparkUtils.saveCoalescedRDDToJsonFile(yearMonthDelaySum,
        //                outputDir + File.separator + "number_of_delays_each_month-year");

        // join all flights in a month, number of airlines each month, and occurances of delays each month
        //        JavaPairRDD<Tuple2<String, String>, Iterable<Long>> allFlightsNumAirlinesDelaysEachMonth =
        //                numberFlightsEachMonth.union(totalsForEachMonth).union(yearMonthDelaySum).groupByKey();

        JavaPairRDD<Tuple2<String, String>, Iterable<Long>> allFlightsNumAirlinesDelaysEachMonth =
                ctx.union(numberFlightsEachMonth, totalsForEachMonth, yearMonthDelaySum)
                        .groupByKey();
        // save out combined data
        SparkUtils.saveCoalescedRDDToJsonFile(allFlightsNumAirlinesDelaysEachMonth,
                outputDir + File.separator + "allFlights_numAirlines_numDelays_by_month");

        //
        // models with most and fewest delays
        //

        // get number of times each tail number has been delayed
        JavaPairRDD<String, Long> tailNumDelayOccurances =
                occurancesOfDelays.mapToPair((PairFunction<FlightInfo, String, Long>) fci -> {
                            // trim N off of tail num
                            String tailNum = fci.getTailNum();

                            // tail num data set doesn't have 'N' as prefix, so remove it
                            if (tailNum != null && tailNum.length() >= 2) {
                                if (!tailNum.equals("NA") && tailNum.charAt(0) == 'N') {
                                    tailNum = tailNum.substring(1, tailNum.length());
                                }
                            }

                            return new Tuple2<>(tailNum, new Long(1));
                        });

        // count them up
        JavaPairRDD<String, Long> tailNumDelayCounts =
                tailNumDelayOccurances.reduceByKey((l1, l2) -> l1 + l2);

        /*
        SparkUtils.saveCoalescedLongRDDToCsvFile(tailNumDelayCounts,
                outputDir + File.separator + "tail_num_occurances");

        SparkUtils.saveCoalescedRDDToJsonFile(tailNumToModelRdd,
                outputDir + File.separator + "tail_num_to_model");
                */

        // join with tailNumToModelRdd
        JavaPairRDD<String, Tuple2<Long, String>> tailNumDelaysAndModel = tailNumDelayCounts.join(
                tailNumToModelRdd);

        // swap tail num for model
        JavaPairRDD<String, Long> modelToDelays =
                tailNumDelaysAndModel.mapToPair((PairFunction<Tuple2<String, Tuple2<Long, String>>, String, Long>) tailNumTup -> new Tuple2<>(
                                tailNumTup._2()
                                        ._2(),
                                tailNumTup._2()
                                        ._1()))
                        .reduceByKey((l1, l2) -> l1 + l2);

        // save out
        SparkUtils.saveCoalescedRDDToJsonFile(modelToDelays,
                outputDir + File.separator + "delays_by_model_number");

        return false;
    }
}
