package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.WellKnownPathTypes;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataQuality;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataTypeClass;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.ColumnQueryDefinition;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryNode;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryQueryExecutor;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.QueryController;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ErrorHistoryColumn;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ProcessedHistoryColumn;
import com.microsoft.opensource.cla.ignition.TagValue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.ingest.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
    private List<AzureKustoHistoryTag> tags; // The list of tags to return with data

    private ConnectionStringBuilder connectionString;

    // A client for querying data
    private ClientImpl              kustoQueryclient;

    // A client for ingesting data in bulks
    private IngestClient            kustoQueuedIngestclient;

    // A client for ingesting row by row
    private StreamingIngestClient   kustoTreamingIngestclient;

    boolean processed = false;
    long maxTSInData = -1;

    public AzureKustoQueryExecutor(GatewayContext context, AzureKustoHistoryProviderSettings settings, List<ColumnQueryDefinition> tagDefs, QueryController controller) {
        this.context = context;
        this.settings = settings;
        this.controller = controller;
        this.tagDefs = tagDefs;

        this.tags = new ArrayList<>();

        initTags();
    }

    /**
     * Initialize the tags we want to query. This will create a list of AzureKustoHistoryTag
     * that provides the fully qualified tag path, aggregation function, and tag to return.
     */
    private void initTags() {
        boolean isRaw = controller.getBlockSize() <= 0;

        for (ColumnQueryDefinition c : tagDefs) {
            HistoryNode tag;

            QualifiedPath qPath = c.getPath();
            String itemId = qPath.getPathComponent(WellKnownPathTypes.Tag);

            TagValue tagValue = new TagValue();
            String driver = qPath.getPathComponent(WellKnownPathTypes.Driver);
            String[] parts = driver.split(":");
            tagValue.setSystemName(parts[0]);
            tagValue.setTagProvider(parts[1]);
            tagValue.setTagPath(itemId);
            String fullTagPath = tagValue.toStringFull();

            if (StringUtils.isBlank(itemId)) {
                // We set the data type to Integer here, because if the column is going to be errored, at least integer types won't cause charts to complain.
                tag = new ErrorHistoryColumn(c.getColumnName(), DataTypeClass.Integer, DataQuality.CONFIG_ERROR);
                logger.debug(controller.getQueryId() + ": The item path '" + c.getPath() + "' does not have a valid tag path component.");
            } else {
                tag = new ProcessedHistoryColumn(c.getColumnName(), isRaw);
                // Set data type to float by default, we can change this later if needed
                ((ProcessedHistoryColumn) tag).setDataType(DataTypeClass.Float);
            }

            tags.add(new AzureKustoHistoryTag(fullTagPath, c.getAggregate(), tag));
        }
    }

    /**
     * Provides the data structure to Ignition where we have our tags stored
     */
    @Override
    public List<? extends HistoryNode> getColumnNodes() {
        List<HistoryNode> nodes = new ArrayList<>();
        for (AzureKustoHistoryTag node : tags) {
            nodes.add(node.getTag());
        }
        return nodes;
    }

    /**
     * Called first to initialize the connection
     */
    @Override
    public void initialize() throws Exception {
        connectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(
                AzureKustoHistoryProviderSettings.ClusterURL.toString(),
                AzureKustoHistoryProviderSettings.ApplicationId.toString(),
                AzureKustoHistoryProviderSettings.ApplicationKey.toString(),
                AzureKustoHistoryProviderSettings.AADTenantId.toString());

        kustoQueryclient = new ClientImpl(connectionString);

        kustoQueuedIngestclient = IngestClientFactory.createClient(connectionString);

        kustoTreamingIngestclient = IngestClientFactory.createStreamingIngestClient(connectionString);
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

        // TODO: Query the data from ADX and add to each to node
        //for (AzureKustoHistoryNode node : columnNodes) {
        //    if(node.valid()) { // Only for tags with valid tag path
        //       List<QualifiedValue> values = new ArrayList<>();
        //       node.getProcessedHistoryTag().put(values);
        //
        //       long resMaxTS = ...;
        //       if (resMaxTS > maxTSInData) {
        //           maxTSInData = resMaxTS;
        //       }
        //   }
        //}

        if (blockSize > 0) {
            // Block data
        } else {
            // Raw data
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