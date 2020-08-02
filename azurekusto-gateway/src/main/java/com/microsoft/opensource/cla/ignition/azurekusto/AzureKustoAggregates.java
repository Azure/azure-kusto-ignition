package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;

/**
 * Represents all of the aggregation functions the provider supports
 */
public enum AzureKustoAggregates implements Aggregate {
    AZUREKUSTO_TOTAL,
    AZUREKUSTO_AVERAGE,
    AZUREKUSTO_TIMEAVERAGE, //Travis, can you explain what that means?
    AZUREKUSTO_COUNT,
    AZUREKUSTO_STDEV,
    AZUREKUSTO_MINIMUM,
    AZUREKUSTO_MAXIMUM,
    AZUREKUSTO_VARIANCE,
    AZUREKUSTO_ANY,
    AZUREKUSTO_DCOUNT,
    AZUREKUSTO_RANGE,
    AZUREKUSTO_DURATIONGOOD,
    AZUREKUSTO_DURATIONBAD,
    AZUREKUSTO_PERCENTGOOD,
    AZUREKUSTO_PERCENTBAD;

    @Override
    public int getId() {
        return ordinal();
    }

    @Override
    public String getName() {
        return name();
        // TODO is this how the UI gets the nice names?

    }

    public String getKqlFunction() {
        if(ordinal() == AZUREKUSTO_TOTAL.ordinal()) {return "sum";}
        if(ordinal() == AZUREKUSTO_AVERAGE.ordinal()) {return "avg";}
        if(ordinal() == AZUREKUSTO_COUNT.ordinal()) {return "count";}
        if(ordinal() == AZUREKUSTO_STDEV.ordinal()) {return "stdev";}
        if(ordinal() == AZUREKUSTO_MINIMUM.ordinal()) {return "min";}
        if(ordinal() == AZUREKUSTO_MAXIMUM.ordinal()) { return "max";}
        if(ordinal() == AZUREKUSTO_VARIANCE.ordinal()) { return "variance";}
        if(ordinal() == AZUREKUSTO_ANY.ordinal()) { return "any";}
        if(ordinal() == AZUREKUSTO_DCOUNT.ordinal()) { return "dcount";}

        return null;
        // TODO - How to communicate the right names for the UI?
        // TODO - Add the rest of the Kusto aggregates
        // TODO - Add the rest of Ignition asks
        //        AZUREKUSTO_TIMEAVERAGE
        //        AZUREKUSTO_RANGE,
        //        AZUREKUSTO_DURATIONGOOD,
        //        AZUREKUSTO_DURATIONBAD,
        //        AZUREKUSTO_PERCENTGOOD,
        //        AZUREKUSTO_PERCENTBAD;
    }

    @Override
    public String getDesc() {
        return "";
    }
}
