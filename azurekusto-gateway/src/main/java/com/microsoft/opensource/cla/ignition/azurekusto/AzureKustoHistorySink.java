package com.microsoft.opensource.cla.ignition.azurekusto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inductiveautomation.ignition.common.StatMetric;
import com.inductiveautomation.ignition.common.i18n.LocalizedString;
import com.inductiveautomation.ignition.gateway.history.*;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.StreamingIngestClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import com.microsoft.opensource.cla.ignition.Utils;

/**
 * Responsible for actually storing the data to ADX. Can either use the
 * built-in store & forward system for Ignition or use its own.
 */
public class AzureKustoHistorySink implements DataSink {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private AzureKustoHistoryProviderSettings settings; // Holds the settings for the current provider, needed to connect to ADX
    private GatewayContext context;
    private String pipelineName;
    private StreamingIngestClient streamingIngestClient;
    private IngestClient queuedClient;
    private String table;
    private String database;
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS");

    private IngestionProperties ingestionProperties;
    public AzureKustoHistorySink(String pipelineName, GatewayContext context, AzureKustoHistoryProviderSettings settings) {
        this.pipelineName = pipelineName;
        this.context = context;
        this.settings = settings;
    }

    @Override
    public String getPipelineName() {
        return pipelineName;
    }

    @Override
    public void startup() {
        String clusterURL = settings.getString(AzureKustoHistoryProviderSettings.ClusterURL);
        String applicationId = settings.getString(AzureKustoHistoryProviderSettings.ApplicationId);
        String applicationKey = settings.getString(AzureKustoHistoryProviderSettings.ApplicationKey);
        String aadTenantId = settings.getString(AzureKustoHistoryProviderSettings.AADTenantId);
        database = settings.getString(AzureKustoHistoryProviderSettings.DatabaseName);

        String dmUrl = Utils.getDMUriFromSetting(clusterURL);
        String engineURL = Utils.getEngineUriFromSetting(clusterURL);

        ConnectionStringBuilder connectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(
                engineURL,
                applicationId,
                applicationKey,
                aadTenantId);
        ConnectionStringBuilder DmConnectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(
                dmUrl,
                applicationId,
                applicationKey,
                aadTenantId);
        try {
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(connectionString);
            queuedClient = IngestClientFactory.createClient(DmConnectionString);
            table = settings.getEventsTableName();
            ingestionProperties = new IngestionProperties(database, table);
            ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.csv);

        } catch (URISyntaxException ex) {
            logger.error("Error on AzureKustoHistorySink startup ", ex);
        }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isAccepting() {
        // TODO: Determine if ADX is accepting data
        return true;
    }

    @Override
    public List<DataSinkInformation> getInfo() {
        // TODO: Determine the status of the ADX history sink
        return Arrays.asList(new HistorySinkStatus());
    }

    @Override
    public QuarantineManager getQuarantineManager() {
        return null; // Not implemented
    }

    /**
     * Called from Ignition when tags change and have data available for storage.
     */
    @Override
    public void storeData(HistoricalData data) throws IOException, IngestionClientException, IngestionServiceException { // TODO Should we fail on error?
        logger.debug("Received data of type '" + data.getClass().toString() + "'");

        List<AzureKustoTagValue> records = new ArrayList<AzureKustoTagValue>();

        List<HistoricalData> dataList;
        if (data instanceof DataTransaction) {
            dataList = ((DataTransaction) data).getData();
        } else {
            dataList = Collections.singletonList(data);
        }

        // Find all of the tags passed in that have data
        logger.debug("History set with '" + dataList.size() + "' row(s)");
        for (HistoricalData d : dataList) {
            if (d instanceof ScanclassHistorySet) {
                ScanclassHistorySet dSet = (ScanclassHistorySet) d;
                logger.debug("Scan class set '" + dSet.getSetName() + "' has '" + dSet.size() + "' tag(s)");
                for (HistoricalTagValue historicalTagValue : dSet) {
                    AzureKustoTagValue tagValue = new AzureKustoTagValue(context, historicalTagValue);
                    logger.trace(tagValue.toString());
                    records.add(tagValue);
                }
            } else if (d instanceof HistoricalTagValue) {
                HistoricalTagValue dValue = (HistoricalTagValue) d;
                AzureKustoTagValue tagValue = new AzureKustoTagValue(context, dValue);
                logger.trace(tagValue.toString());
                records.add(tagValue);
            }
        }
        ingestRecords(records);
    }

    void ingestRecords(List<AzureKustoTagValue> records) throws IngestionClientException, IngestionServiceException, IOException {
        ByteArrayOutputStream bis = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bis);
        CsvWriter csvWriter = new CsvWriter(gzipOutputStream, new CsvWriterSettings());
        // Write as csv stream
        if (records.size() > 0) {
            // TODO how much data can one such batch have - maybe we should write straight to blob
            logger.debug("Logging " + records.size() + " records");
            for (AzureKustoTagValue record: records) {
                Object[] recordAsObjects = new Object[8];
                csvWriter.writeRow();
                if(record.getSystemName() != null) recordAsObjects[0] = record.getSystemName();
                if(record.getTagProvider() != null) recordAsObjects[1] = record.getTagProvider();
                if(record.getTagPath() != null) recordAsObjects[2] = record.getTagPath();
                Object value = record.getValue();
                if(value != null){
                    ObjectMapper objectMapper = new ObjectMapper();
                    String valueAsJson = objectMapper.writeValueAsString(value);
                    recordAsObjects[3] = valueAsJson;

                    if(value instanceof Double) {
                        recordAsObjects[4] = (Double)value;
                    }
                    else if(value instanceof Integer) {
                        recordAsObjects[5] = (Integer)value;
                    }
                }

                if(record.getTimestamp() != null){
                    String formattedDate = simpleDateFormat.format(record.getTimestamp());
                    recordAsObjects[6] = formattedDate;
                }
                if(record.getQuality() != null) recordAsObjects[7] = record.getQuality();
                csvWriter.writeRow(recordAsObjects);
            }
        }
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(new ByteArrayInputStream(bis.toByteArray()),false);
        streamSourceInfo.setCompressionType(CompressionType.gz);

        // Can change here to streaming
        queuedClient.ingestFromStream(streamSourceInfo, ingestionProperties);
    }

    @Override
    public boolean acceptsData(HistoryFlavor historyFlavor) {
        return historyFlavor.equals(HistoryFlavor.SQLTAG);
    }

    @Override
    public boolean isLicensedFor(HistoryFlavor historyFlavor) {
        return true;
    }

    protected class HistorySinkStatus implements DataSinkInformation {

        @Override
        public DataStoreStatus getDataStoreStatus() {
            return null;
        }

        @Override
        public String getDescriptionKey() {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return isAccepting();
        }

        @Override
        public boolean isDataStore() {
            return false;
        }

        @Override
        public List<LocalizedString> getMessages() {
            return null;
        }

        @Override
        public StatMetric getStorageMetric() {
            return null;
        }
    }
}
