package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.Path;
import com.inductiveautomation.ignition.common.expressions.DefaultFunctionFactory;
import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;
import com.inductiveautomation.ignition.common.sqltags.history.ReturnFormat;
import com.inductiveautomation.ignition.common.sqltags.history.TagHistoryQueryParams;
import com.inductiveautomation.ignition.common.util.Flags;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import static com.inductiveautomation.ignition.common.expressions.DefaultFunctionFactory.*;

public class KustoTestTagHistoryQueryParams implements TagHistoryQueryParams {
    public List<? extends Path> getPaths()
    {
        return null;
    }

    public List<String> getAliases()
    {
        return null;
    }

    public Date getStartDate()
    {
//        return new Date(DateTime.now().toDate().getTime() - 10000000);
        return new Date(0);
    }

    public Date getEndDate()
    {
        return DateTime.now().toDate();
    }

    public int getReturnSize()
    {
        return 20;
    }

    public Aggregate getAggregationMode()
    {
        return AzureKustoAggregates.AZUREKUSTO_AVERAGE;
    }

    public List<Aggregate> getColumnAggregationModes()
    {return null;}

    public ReturnFormat getReturnFormat()
    {
        return null;
    }

    public Flags getQueryFlags()
    {
        return null;
    }
}
