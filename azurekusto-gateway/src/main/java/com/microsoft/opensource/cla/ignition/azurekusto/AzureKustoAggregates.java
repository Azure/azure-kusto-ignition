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
    }

    @Override
    public String getDesc() {
        return "";
    }
}
