package com.microsoft.opensource.cla.ignition.azurekusto;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;

public class KustoTest {

    public static void main(String[] args) throws IngestionClientException, IOException, IngestionServiceException, DataClientException, DataServiceException, URISyntaxException {

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

        AzureKustoHistorySink kusto = new AzureKustoHistorySink("kusto", null, settings);
        kusto.startup();
        ArrayList<AzureKustoTagValue> recs = new ArrayList<>();
        AzureKustoTagValue azureKustoTagValue = new AzureKustoTagValue();
        azureKustoTagValue.setSystemName("kustoIgnitoin");
        azureKustoTagValue.setTagProvider("ohad and uri");
        azureKustoTagValue.setTagPath("toKusto");
        azureKustoTagValue.setValue(new Object(){
            public String name = "travis";
            public int x = 1;
        });
        azureKustoTagValue.setTimestamp(new Date());
        azureKustoTagValue.setQuality(1000000);
        recs.add(azureKustoTagValue);

        kusto.ingestRecords(recs);

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                clusterName,
                appId,
                appKey,
                appTenant
               );

        ClientImpl client = new ClientImpl(csb);

        String queryText = "StormEvents| summarize count() by State, startofmonth(StartTime)";

        // in case we want to pass client request properties
        //ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        //clientRequestProperties.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(1));

        KustoOperationResult results = client.execute(databaseName, queryText
        //        , clientRequestProperties
        );
        KustoResultSetTable mainTableResult = results.getPrimaryResults();
        System.out.println(String.format("Kusto sent back %s rows.", mainTableResult.count()));
    }
}
