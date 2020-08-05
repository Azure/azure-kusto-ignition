package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.WellKnownPathTypes;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.browsing.BrowseResults;
import com.inductiveautomation.ignition.common.browsing.Result;
import com.inductiveautomation.ignition.common.browsing.TagResult;
import com.inductiveautomation.ignition.common.model.values.BasicQualifiedValue;
import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataQuality;
import com.inductiveautomation.ignition.common.sqltags.model.types.TagQuality;
import com.inductiveautomation.ignition.common.util.Timeline;
import com.inductiveautomation.ignition.common.util.TimelineSet;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import com.inductiveautomation.ignition.gateway.sqltags.history.TagHistoryProvider;
import com.inductiveautomation.ignition.gateway.sqltags.history.TagHistoryProviderInformation;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.ColumnQueryDefinition;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.QueryController;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.opensource.cla.ignition.Utils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Represents an instance of the Azure Kusto history provider. The provider creates a
 * data sink for storage and a query executor for retrieval. Both the sink and provider
 * must have the same name.
 */
public class AzureKustoHistoryProvider implements TagHistoryProvider {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String name;
    private GatewayContext context;
    private AzureKustoHistoryProviderSettings settings;
    private AzureKustoHistorySink sink;
    private ClientImpl kustoQueryClient; // A client for querying data

    public AzureKustoHistoryProvider(GatewayContext context, String name, AzureKustoHistoryProviderSettings settings) {
        this.name = name;
        this.context = context;
        this.settings = settings;
    }

    @Override
    public void startup() {

        try {
            // Create a new data sink with the same name as the provider to store data
            sink = new AzureKustoHistorySink(name, context, settings);
            context.getHistoryManager().registerSink(sink);

            // Create a Kusto client
            ConnectToKusto();

        } catch (Throwable e) {
            logger.error("Error registering Azure Kusto history sink", e);
        }
    }

    public void ConnectToKusto() throws URISyntaxException {
        String clusterURL = settings.getClusterURL();
        String applicationId = settings.getString(AzureKustoHistoryProviderSettings.ApplicationId);
        String applicationKey = settings.getString(AzureKustoHistoryProviderSettings.ApplicationKey);
        String aadTenantId = settings.getString(AzureKustoHistoryProviderSettings.AADTenantId);

        ConnectionStringBuilder connectionString;

        connectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(
                clusterURL,
                applicationId,
                applicationKey,
                aadTenantId);

        kustoQueryClient = new ClientImpl(connectionString);
    }

    @Override
    public void shutdown() {
        try {
            // Unregister the data sink so it doesn't show up in the list to choose from
            context.getHistoryManager().unregisterSink(sink, false);
        } catch (Throwable e) {
            logger.error("Error shutting down Azure Kusto history sink", e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Aggregate> getAvailableAggregates() {
        // TODO: Determine the list of aggregate functions that the module supports
        return Arrays.asList(AzureKustoAggregates.values());
    }

    @Override
    public ProfileStatus getStatus() {
        // TODO: Determine ADX status for retrieval
        return ProfileStatus.RUNNING;
    }

    @Override
    public TagHistoryProviderInformation getStatusInformation() {
        return TagHistoryProviderInformation.newBuilder().allowsStorage(false).status(getStatus()).name(getName())
                .build();
    }

    @Override
    public AzureKustoQueryExecutor createQuery(List<ColumnQueryDefinition> tags, QueryController queryController) {
        logger.debug("createQuery(tags, queryController) called.  tags: " + tags.toString()
                + ", queryController: " + queryController.toString());

        return new AzureKustoQueryExecutor(context, settings, tags, queryController);
    }

    /**
     * Browses for tags available in ADX. Returns a tree of tags. The function is called several times,
     * lazy loading, from a specific starting point.
     */
    @Override
    public BrowseResults<Result> browse(QualifiedPath qualifiedPath, BrowseFilter browseFilter) {
        logger.debug("browse(qualifiedPath, browseFilter) called.  qualifiedPath: " + qualifiedPath.toString()
                + ", browseFilter: " + (browseFilter == null ? "null" : browseFilter.toString()));

        BrowseResults<Result> result = new BrowseResults<>();
        ArrayList<Result> list = new ArrayList<>();

        // First, we need to find the starting point based on history provider, system name, tag provider, and tag path
        String histProv = qualifiedPath.getPathComponent(WellKnownPathTypes.HistoryProvider);
        String systemName = null;
        String tagProvider = null;
        String driver = qualifiedPath.getPathComponent(WellKnownPathTypes.Driver);
        if (driver != null) {
            String[] parts = driver.split(":");
            systemName = parts[0];
            tagProvider = parts[1];
        }
        String tagPath = qualifiedPath.getPathComponent(WellKnownPathTypes.Tag);

        String queryData = settings.getEventsTableName() + "| where timestamp > ago(5d) \n";

        if (systemName == null)
        {
            queryData += "| extend current = systemName";
            queryData += "| extend child = tagPath";
            queryData += "| summarize countSubNodes = dcount(child) by current, systemName, tagProvider, tagPath=''";
        }
        else if (tagPath == null)
        {
            queryData += "| where systemName == \"" + systemName + "\"";
            queryData += "| where tagProvider == \"" + tagProvider + "\"";
            queryData += "| parse tagPath with current '/' child";
            queryData += "| where child !contains '/'";
            queryData += "| summarize countSubNodes = dcount(child) by tagPath = current, systemName, tagProvider";
        }
        else
        {
            queryData += "| where systemName == '" + systemName + "'";
            queryData += "| where tagProvider == '" + tagProvider + "'";
            queryData += "| where tagPath startswith '" + tagPath + "'";
            queryData += "| parse tagPath with  '" + tagPath +"' " + "residue";
            queryData += "| parse residue with current '/' child";
            queryData += "| extend current = iff(isnotempty(current), current, residue)";
            queryData += "| extend group = strcat('" + tagPath + "', current)";
            queryData += "| where child !contains '/' and isnotempty(residue)";
            queryData += "| summarize countSubNodes = dcount(child) by systemName, tagProvider, tagPath = group";
        }

        queryData += "| extend hasChildren = countSubNodes > 1 | sort by systemName, tagProvider, tagPath, hasChildren asc | project systemName, tagProvider, tagPath, hasChildren";

        String query = queryData;
        logger.debug("Issuing query:" + query);

        try {
            KustoOperationResult results = kustoQueryClient.execute(settings.getDatabaseName(), query);
            KustoResultSetTable mainTableResult = results.getPrimaryResults();

            while (mainTableResult.next()) {
                String systemNameFromRecord = mainTableResult.getString("systemName");
                String tagProviderFromRecord = mainTableResult.getString("tagProvider");
                String tagPathFromRecord = mainTableResult.getString("tagPath");
                Boolean boolHasChildren = mainTableResult.getBoolean("hasChildren");
                logger.debug(
                        "Browsing: System:" + systemName +
                                " tagProvider:" + tagProvider +
                                " tagPath:" + tagPath);

                TagResult tagResult = new TagResult();
                tagResult.setHasChildren(boolHasChildren);
                QualifiedPath.Builder builder = new QualifiedPath.Builder().set(WellKnownPathTypes.HistoryProvider, histProv);
                if(systemNameFromRecord != null && !systemNameFromRecord.isEmpty()) {
                    builder.setDriver(systemNameFromRecord + ":" + tagProviderFromRecord);
                }
                if(tagPathFromRecord != null && !tagPathFromRecord.isEmpty()) {
                    builder.setTag(tagPathFromRecord);
                }
                tagResult.setPath(builder.build());
                list.add(tagResult);
            }
        }
        catch (Exception e)
        {
            logger.error("Issuing query failed: returning empty results: " + query);
        }

        result.setResults(list);
        result.setTotalAvailableResults(list.size());
        result.setResultQuality(TagQuality.GOOD);
        return result;
    }

    @Override
    public TimelineSet queryDensity(
            List<QualifiedPath> tags,
            Date                startDate,
            Date                endDate,
            String              queryId) throws Exception {
        logger.debug("queryDensity(tags, startDate, endDate, queryId) called.  tags: " + tags.toString()
                + ", startDate: " + startDate.toString() + ", endDate: " + endDate.toString() + ", queryId: " + queryId);

        // TODO: Find the data density for the tags within the given range. Density is just an ordered list of
        //  timestamps for the given range with a weight.
        ArrayList<Timeline> timelines = new ArrayList<>();
        TimelineSet timelineSet = new TimelineSet(timelines);
        return timelineSet;
    }
}
