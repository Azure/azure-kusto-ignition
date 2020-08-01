package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.history.TagHistoryQueryParams;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.QueryController;

public class KustoQueryController implements QueryController {
    public String getQueryId() {
        System.out.println("getQueryId called");

        return "Kusto Test driver";
    }

    public long getBlockId(long var1) {
        System.out.println("getBlockId called");

        return 5;
    }

    public long getBlockSize() {
        System.out.println("getBlockSize called");

        return 3;
    }

    public long getQueryStart() {
        System.out.println("getQueryStart called");

        return 3;
    }


    public long getQueryEnd() {
        System.out.println("getQueryEnd called");

        return 3;
    }


    public TagHistoryQueryParams getQueryParameters() {
        System.out.println("getQueryParameters called");

        return null;
    }
}
