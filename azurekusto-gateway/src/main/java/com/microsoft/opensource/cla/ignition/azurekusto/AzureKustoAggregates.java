package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;

/**
 * Represents all of the aggregation functions the provider supports
 */

//"MinMax" - will return two entries per time slice - the min and the max.
//"Average" - will return the time-weighted average value of all samples in that time slice.
//"LastValue" - returns the most recent actual value to the end of the window. Note that if a value does not exist in this window, a 0 will be returned in cases where interpolation is turned off.
//"SimpleAverage" - returns the simple mathematical average of the values - ((V1+V2+...+Vn)/n)
//"Maximum" - the maximum value of the window.
//"Minimum" - the minimum value of the window.
//"DurationOn" - the time, in seconds, that a value has been boolean true
//"DurationOff" - the time, in seconds, that a value has been boolean false
//"CountOn" - the number of times the value has transitioned to boolean true
//"CountOff" - the number of times the value has transitioned to boolean false
//"Count" - the number of "good", non-interpolated values per window.
//"Range" - the difference between the min and max
//"Variance" - the variance for "good", non-interpolated values. Does not time weight.
//"StdDev" - the standard deviation for "good", non-interpolated values. Does not time weight.
//"PctGood" - the percentage of time the value was good.
//"PctBad" - the percentage of time the value was bad.

public enum AzureKustoAggregates implements Aggregate {
    AZUREKUSTO_MINMAX("MinMax", "min"),
    AZUREKUSTO_LASTVALUE("LastValue", "any"), // TODO
    AZUREKUSTO_TOTAL("Total", "total"),
    AZUREKUSTO_DCOUNT("DCount", "dcount"),
    AZUREKUSTO_AVERAGE("SimpleAverage", "avg"),
    AZUREKUSTO_TIMEAVERAGE("Average", "avg"), // TODO
    AZUREKUSTO_COUNT("Count", "count"),
    AZUREKUSTO_STDEV("StdDev", "stddev"),
    AZUREKUSTO_MINIMUM("Mininum", "min"),
    AZUREKUSTO_MAXIMUM("Maximum", "max"),
    AZUREKUSTO_VARIANCE("Variance", "variance"),
    AZUREKUSTO_COUNTON("CountOn", "count"), // TODO
    AZUREKUSTO_COUNTOFF("CountOff", "count"), // TODO
    AZUREKUSTO_DURATIONON("DurationOn", "count"), // TODO
    AZUREKUSTO_DURAITONOFF("DurationOff", "count"), // TODO
    AZUREKUSTO_RANGE("Range", "avg"), // TODO
    AZUREKUSTO_PERCENTGOOD("PctGood", "avg"), // TODO
    AZUREKUSTO_PERCENTBAD("PctBad", "avg"); // TODO

    private String ignitionAggregate;
    private String kqlFunction;
    AzureKustoAggregates(String ignitionAggregate, String kqlFunction){
        this.ignitionAggregate = ignitionAggregate;
        this.kqlFunction = kqlFunction;
    }

    @Override
    public int getId() {
        return ordinal();
    }

    @Override
    public String getName() {
        return name();
    }

    public String getIgnitionAggregate() {
        return ignitionAggregate;
    }

    public String getKqlFunction() {
        return kqlFunction;
    }

    public static String getKqlFunction(Aggregate aggregate) {
        for(AzureKustoAggregates azureKustoAggregate : AzureKustoAggregates.values()){
            if(azureKustoAggregate.getIgnitionAggregate().equals(aggregate.getName())){
                return azureKustoAggregate.getKqlFunction();
            }
        }

        return "avg";
    }

    @Override
    public String getDesc() {
        return "";
    }
}
