package cs455.flightdata.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

public interface FlightAnalysisIface {

    boolean executeAnalysis(JavaSparkContext ctx, JavaRDD<FlightInfo> lines, String outputDir);

}
