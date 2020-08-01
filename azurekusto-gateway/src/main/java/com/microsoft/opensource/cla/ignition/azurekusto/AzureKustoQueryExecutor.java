package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.WellKnownPathTypes;
import com.inductiveautomation.ignition.common.model.values.BasicQualifiedValue;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataQuality;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataTypeClass;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.ColumnQueryDefinition;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryNode;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryQueryExecutor;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.QueryController;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ErrorHistoryColumn;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ProcessedHistoryColumn;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.StreamingIngestClient;
import com.microsoft.opensource.cla.ignition.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

/**
 * Responsible for actually querying the data from ADX. The query controller
 * provides the list of tags and settings for querying the data. We can either
 * query for raw data or break up the data into intervals. If we break data up,
 * we can apply an aggregation function against the intervals, such as average.
 */
public class AzureKustoQueryExecutor implements HistoryQueryExecutor {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private GatewayContext context;
    private AzureKustoHistoryProviderSettings settings; // Holds the settings for the current provider, needed to connect to ADX
    private QueryController controller; // Holds the settings for what the user wants to query
    private List<ColumnQueryDefinition> tagDefs; // Holds the definition of each tag
    private Map<AzureKustoTag, AzureKustoHistoryTag> tags; // The list of tags to return with data

    private ConnectionStringBuilder connectionString;
    private ClientImpl kustoQueryClient; // A client for querying data
    private IngestClient kustoQueuedIngestClient; // A client for ingesting data in bulks
    private StreamingIngestClient kustoStreamingIngestClient; // A client for ingesting row by row

    boolean processed = false;
    long maxTSInData = -1;

    public AzureKustoQueryExecutor(GatewayContext context, AzureKustoHistoryProviderSettings settings, List<ColumnQueryDefinition> tagDefs, QueryController controller) {
        this.context = context;
        this.settings = settings;
        this.controller = controller;
        this.tagDefs = tagDefs;
        this.tags = new HashMap<>();

        initTags();
    }

    /**
     * Initialize the tags we want to query. This will create a list of AzureKustoHistoryTag
     * that provides the fully qualified tag path, aggregation function, and tag to return.
     */
    private void initTags() {
        boolean isRaw = controller.getBlockSize() <= 0;

        for (ColumnQueryDefinition c : tagDefs) {
            HistoryNode historyTag;

            QualifiedPath qPath = c.getPath();
            String driver = qPath.getPathComponent(WellKnownPathTypes.Driver);
            String[] parts = driver.split(":");
            String systemName = parts[0];
            String tagProvider = parts[1];
            String tagPath = qPath.getPathComponent(WellKnownPathTypes.Tag);

            AzureKustoTag tag = new AzureKustoTag(systemName, tagProvider, tagPath);

            if (StringUtils.isBlank(tagPath)) {
                // We set the data type to Integer here, because if the column is going to be errored, at least integer types won't cause charts to complain.
                historyTag = new ErrorHistoryColumn(c.getColumnName(), DataTypeClass.Integer, DataQuality.CONFIG_ERROR);
                logger.debug(controller.getQueryId() + ": The item path '" + c.getPath() + "' does not have a valid tag path component.");
            } else {
                historyTag = new ProcessedHistoryColumn(c.getColumnName(), isRaw);
                // Set data type to float by default, we can change this later if needed
                ((ProcessedHistoryColumn) historyTag).setDataType(DataTypeClass.Float);
            }

            tags.put(tag, new AzureKustoHistoryTag(tag, c.getAggregate(), historyTag));
        }
    }

    /**
     * Provides the data structure to Ignition where we have our tags stored
     */
    @Override
    public List<? extends HistoryNode> getColumnNodes() {
        List<HistoryNode> nodes = new ArrayList<>();
        for (AzureKustoTag tag : tags.keySet()) {
            nodes.add(tags.get(tag).getHistoryTag());
        }
        return nodes;
    }

    /**
     * Called first to initialize the connection
     */
    @Override
    public void initialize() throws Exception {
        String clusterURL = settings.getClusterURL();
        String applicationId = settings.getString(AzureKustoHistoryProviderSettings.ApplicationId);
        String applicationKey = settings.getString(AzureKustoHistoryProviderSettings.ApplicationKey);
        String aadTenantId = settings.getString(AzureKustoHistoryProviderSettings.AADTenantId);

        connectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(
                clusterURL,
                applicationId,
                applicationKey,
                aadTenantId);

        kustoQueryClient = new ClientImpl(connectionString);
    }

    @Override
    public int getEffectiveWindowSizeMS() {
        return 0; // Always return 0 to allow for any kind of window sizes
    }

    /**
     * Called after initialization to start reading the data. This is where the bulk
     * of the work happens.
     */
    @Override
    public void startReading() throws Exception {
        int blockSize = (int) controller.getBlockSize();
        Date startDate = controller.getQueryParameters().getStartDate();
        Date endDate = controller.getQueryParameters().getEndDate();

        logger.debug("startReading(blockSize, startDate, endDate) called.  blockSize: " + blockSize
                + ", startDate: " + startDate.toString() + ", endDate: " + endDate.toString());

        String queryPrefix =
                "let blocks = " + blockSize + ";\n" +
                        "let startTime = " + Utils.getDateLiteral(startDate) + ";\n" +
                        "let endTime = " + Utils.getDateLiteral(endDate) + ";\n";
        String queryData = settings.getEventsTableName() + "| where timestamp between(startTime..endTime) ";

        queryData += "| where ";
        AzureKustoTag[] tagKeys = tags.keySet().toArray(new AzureKustoTag[]{});
        for(int i = 0; i < tagKeys.length; i++){
            AzureKustoTag tag = tagKeys[i];
            queryData += "(systemName has \"" + tag.getSystemName() + "\" and tagProvider has \"" + tag.getTagProvider() + "\" and tagPath has \"" + tag.getTagPath() + "\")";
            if(i < (tagKeys.length - 1)){
                queryData += " or ";
            }
        }

        String querySuffix = "| sort by systemName, tagProvider, tagPath, timestamp asc";

        // TODO: Implement all aggregate functions
        if (blockSize > 0) {
            // Block data, use aggregate function
            String function = ((AzureKustoAggregates)controller.getQueryParameters().getAggregationMode()).getKqlFunction();

            queryData = queryData + "| summarize value = " + function + "(value_double), quality = min(quality) by systemName, tagProvider, tagPath, bin_at(timestamp, 1millisecond * blocks, startTime)";
        }

        String query = queryPrefix + queryData + querySuffix;
        logger.debug("Issuing query:" + query);

        KustoOperationResult results = kustoQueryClient.execute(settings.getDatabaseName(), query);
        KustoResultSetTable mainTableResult = results.getPrimaryResults();

        while (mainTableResult.next()) {
            String systemName = mainTableResult.getString("systemName");
            String tagProvider = mainTableResult.getString("tagProvider");
            String tagPath = mainTableResult.getString("tagPath");
            AzureKustoTag tag = new AzureKustoTag(systemName, tagProvider, tagPath);

            Object value = mainTableResult.getObject("value");
            Timestamp timestamp = mainTableResult.getTimestamp("timestamp");
            Integer quality = mainTableResult.getInt("quality");

            logger.debug(
                    "Reading: System:" + systemName +
                            " tagProvider:" + tagProvider +
                            " tagPath:" + tagPath +
                            " Value:" + value +
                            " timestamp:" + timestamp);

            tags.get(tag).getProcessedHistoryTag().put(new BasicQualifiedValue(value, DataQuality.fromIntValue(quality), new Date(timestamp.getTime())));
            if (timestamp.getTime() > maxTSInData) {
                maxTSInData = timestamp.getTime();
            }
        }
    }

    /**
     * Called after start reading to determine if there is more data to read
     */
    @Override
    public boolean hasMore() {
        return !processed;
    }

    /**
     * Called after we have no more data to read. Process data if needed.
     */
    @Override
    public long processData() throws Exception {
        processed = true;
        return maxTSInData;
    }

    /**
     * Called after we have processed data for any clean up
     */
    @Override
    public void endReading() {

    }

    @Override
    public long nextTime() {
        return Long.MAX_VALUE;
    }
}