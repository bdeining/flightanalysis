package cs455.flightdata.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by scrhoads on 4/19/17.
 */
public interface FlightAnalysisIface {

    boolean executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> lines, String outputDir);

}
