import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;

public class KustoTest {

    public static void main(String[] args) {

        String clusterName = "https://ignitionadxpoc.eastus.kusto.windows.net";
        String databaseName = "Contoso";

        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    clusterName,
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
