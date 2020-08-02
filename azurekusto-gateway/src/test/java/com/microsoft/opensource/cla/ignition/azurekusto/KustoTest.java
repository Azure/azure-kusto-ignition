package com.microsoft.opensource.cla.ignition.azurekusto;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.WellKnownPathTypes;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.browsing.BrowseResults;
import com.inductiveautomation.ignition.common.browsing.Result;
import com.inductiveautomation.ignition.common.sqltags.history.TagHistoryQueryParams;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.ColumnQueryDefinition;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryNode;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.QueryController;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;

public class KustoTest{
    private static void PrintResults(BrowseResults<com.inductiveautomation.ignition.common.browsing.Result> results)
    {
        for (com.inductiveautomation.ignition.common.browsing.Result result : results.getResults())
        {
            System.out.println(
                "path:" + result.getPath() +
                " display path:" + result.getDisplayPath() +
                " type:" + result.getType() +
                " children:" + result.hasChildren());
        }
    }

    public static void main(String[] args) throws Exception {

        // Ingest test without context
        String clusterName = System.getProperty("cluster","https://ignitionadxpoc.eastus.kusto.windows.net");
        String databaseName = System.getProperty("database","Contoso");
        String appId = System.getProperty("appId");
        String appKey = System.getProperty("appKey");
        String appTenant = System.getProperty("appTenant");
        AzureKustoHistoryProviderSettings settings = new AzureKustoHistoryProviderSettings();
        settings.setString(AzureKustoHistoryProviderSettings.ClusterURL, clusterName);
        settings.setString(AzureKustoHistoryProviderSettings.ApplicationId, appId);
        settings.setString(AzureKustoHistoryProviderSettings.ApplicationKey, appKey);
        settings.setString(AzureKustoHistoryProviderSettings.AADTenantId, appTenant);
        settings.setString(AzureKustoHistoryProviderSettings.DatabaseName, databaseName);

        //
        // Test sink
        //
        AzureKustoHistorySink kusto = new AzureKustoHistorySink("kusto", null, settings);
        kusto.startup();
        ArrayList<AzureKustoTagValue> recs = new ArrayList<>();
        AzureKustoTag tag = new AzureKustoTag("kustoIgnitoin", "ohad and uri","toKusto");
        AzureKustoTagValue tagValue = new AzureKustoTagValue(tag, new Object(){
            public String name = "travis";
            public int x = 1;
        }, new Date(), 1000000);
        recs.add(tagValue);

        //kusto.ingestRecords(recs);


        QualifiedPath.Builder builder = new QualifiedPath.Builder().set(WellKnownPathTypes.HistoryProvider, "ADX");

        //
        // Test provider
        //

        AzureKustoHistoryProvider provider = new AzureKustoHistoryProvider(null, "ADX", settings);

        provider.ConnectToKusto();

        BrowseFilter browseFilter = new BrowseFilter();

        BrowseResults<com.inductiveautomation.ignition.common.browsing.Result> browsResults = null;

        QualifiedPath qp = builder.build();
        browsResults = provider.browse(qp, browseFilter);
        PrintResults(browsResults);

        builder.setDriver("Ignition-Azure-Kusto-Test:default");
        qp = builder.build();
        browsResults = provider.browse(qp, browseFilter);
        PrintResults(browsResults);

        builder.setTag("Ramp");
        qp = builder.build();
        browsResults = provider.browse(qp, browseFilter);
        PrintResults(browsResults);

        builder = new QualifiedPath.Builder().set(WellKnownPathTypes.HistoryProvider, "ADX").setDriver("Ignition-Azure-Kusto-Test:default");
        builder.setTag("Ramp/Ramp1");
        qp = builder.build();
        browsResults = provider.browse(qp, browseFilter);
        PrintResults(browsResults);

        builder = new QualifiedPath.Builder().set(WellKnownPathTypes.HistoryProvider, "ADX").setDriver("Ignition-Azure-Kusto-Test:default");
        builder.setTag("Ramp/Ramp1/Subramp");
        qp = builder.build();
        browsResults = provider.browse(qp, browseFilter);
        PrintResults(browsResults);

        //
        // Test executor
        //

        //provider.createQuery

        builder = new QualifiedPath.Builder().set(
                WellKnownPathTypes.HistoryProvider, "ADX").setDriver(
                "Ignition-Azure-Kusto-Test:default").setTag("Ramp/Ramp1");

        QualifiedPath q0 = builder.build();

        ColumnQueryDefinition c0 = new ColumnQueryDefinition(q0, null);
        ColumnQueryDefinition c1 = new ColumnQueryDefinition(q0, AzureKustoAggregates.AZUREKUSTO_AVERAGE);

        QueryController controller = new KustoQueryController();
        List<ColumnQueryDefinition> tagDefs = new ArrayList<ColumnQueryDefinition>();

        tagDefs.add(c0);
        tagDefs.add(c1);

        AzureKustoQueryExecutor kustoExecutor = new AzureKustoQueryExecutor(null, settings,
                tagDefs,
                controller);

        kustoExecutor.initialize();

        kustoExecutor.startReading();

        for (HistoryNode hn : kustoExecutor.getColumnNodes())
        {
            System.out.println("Found record: " + hn.getName() + " type: " + hn.getDataType() + " value:" + hn.getValue(0,0) );
        }


        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                clusterName,
                appId,
                appKey,
                appTenant
               );

        ClientImpl client = new ClientImpl(csb);

        Date dNow = DateTime.now().toDate();
        Date dEarlier = new Date(0);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS");

        String queryText = "Events| where timestamp between(datetime(" +
                simpleDateFormat.format(dEarlier) + ")..datetime(" +
                simpleDateFormat.format(dNow) + "))";

        System.out.println(queryText);

        // in case we want to pass client request properties
        //ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        //clientRequestProperties.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(1));

        KustoOperationResult queryResults = client.execute(databaseName, queryText
        //        , clientRequestProperties
        );
        KustoResultSetTable mainTableResult = queryResults.getPrimaryResults();
        System.out.println(String.format("Kusto sent back %s rows.", mainTableResult.count()));


        mainTableResult = queryResults.getPrimaryResults();

        while (mainTableResult.next()){
            String system = mainTableResult.getString("systemName");
            String tagProvider = mainTableResult.getString("tagProvider");
            String tagPath = mainTableResult.getString("tagPath");
            Object value = mainTableResult.getObject("value");

            Double value_double = null;
            Integer value_integer = null;
            if (mainTableResult.getObject("value_double")!= null) {
                value_double = mainTableResult.getDouble("value_double");
            }
            if (mainTableResult.getObject("value_integer")!= null) {
                value_integer = mainTableResult.getInt("value_integer");
            }

            Timestamp timestamp = mainTableResult.getTimestamp("timestamp");

            System.out.println(
                            "System:" + system +
                            " tagProvider:" +  tagProvider +
                            " tagPath:" +  tagPath +
                            " Value:" +  value +
                            " value_double:" +  value_double +
                            " value_integer:" +  value_integer +
                            " timestamp:" + timestamp);

            try {
                LocalDateTime ktimestamp = mainTableResult.getKustoDateTime("timestamp"); //TODO - Fix this with format 2020-07-31T21:35:23.00006Z
            } catch (Exception e) {
            }
        }
    }
}
