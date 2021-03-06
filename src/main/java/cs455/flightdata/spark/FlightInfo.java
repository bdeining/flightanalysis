package cs455.flightdata.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class FlightInfo implements Serializable {
    private String cancelCode;

    private Integer isCancelled;

    private String year;

    private String month;

    private String uniqueCarrierCode;

    private String flightDelay;

    private String tailNum;

    private String distance;

    private static final long serialVersionUID = 1L;

    private static Map<String, String> codeToStringMap = new HashMap<>();

    static {
        codeToStringMap.put("A", "Carrier");
        codeToStringMap.put("B", "Weather");
        codeToStringMap.put("C", "NAS");
        codeToStringMap.put("D", "Security");
        codeToStringMap.put("NA", "Not Available");
    }

    public String getCancelCode() {
        return cancelCode;
    }

    public String getCancelCodeHumanReadableString() {
        return codeToStringMap.get(getCancelCode());
    }

    public void setCancelCode(String cancelCode) {
        this.cancelCode = cancelCode.toUpperCase();
    }

    public Integer getIsCancelled() {
        return isCancelled;
    }

    public void setIsCancelled(String isCancelled) {
        try {
            this.isCancelled = Integer.parseInt(isCancelled);
        } catch (NumberFormatException nfe) {
            this.isCancelled = 0;
        }
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getUniqueCarrierCode() {
        return uniqueCarrierCode;
    }

    public void setUniqueCarrierCode(String uniqueCarrierCode) {
        this.uniqueCarrierCode = uniqueCarrierCode;
    }

    public String getFlightDelay() {
        return flightDelay;
    }

    public void setFlightDelay(String flightDelay) {
        this.flightDelay = flightDelay;
    }

    public String getTailNum() {
        return tailNum;
    }

    public void setTailNum(String tailNum) {
        this.tailNum = tailNum;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public static boolean hasDelay(String flightDelay) {
        try {
            Long delayTime = Long.parseLong(flightDelay);
            return delayTime > 15;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
