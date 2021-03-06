package com.microsoft.opensource.cla.ignition.azurekusto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inductiveautomation.ignition.common.StatMetric;
import com.inductiveautomation.ignition.common.i18n.LocalizedString;
import com.inductiveautomation.ignition.gateway.history.*;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.StreamingIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.opensource.cla.ignition.Utils;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

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
        String clusterURL = settings.getClusterURL();
        String applicationId = settings.getApplicationId();
        String applicationKey = settings.getApplicationKey();
        String aadTenantId = settings.getAADTenantId();
        database = settings.getDatabaseName();

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
            ClientImpl client = new ClientImpl(connectionString);

            try {
                KustoOperationResult result = client.execute(database, ".show table " + table);
            } catch (Throwable ex) {
                try {
                    client.execute(database, ".create table " + table + " ( systemName:string, tagProvider:string, tagPath:string, value:dynamic, value_double:real, value_integer:int, timestamp:datetime, quality:int)");
                } catch (Throwable ex2) {
                    logger.error("Error creating table '" + table + "'", ex2);
                }
            }
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(connectionString);
            queuedClient = IngestClientFactory.createClient(DmConnectionString);
            table = settings.getTableName();
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
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(gzipOutputStream);
        CsvWriter csvWriter = new CsvWriter(outputStreamWriter, new CsvWriterSettings());
        // Write as csv stream
        if (records.size() > 0) {
            // TODO how much data can one such batch have - maybe we should write straight to blob
            logger.debug("Logging " + records.size() + " records");
            for (AzureKustoTagValue record : records) {
                Object[] recordAsObjects = new Object[8];
                csvWriter.writeRow();
                if (record.getTag().getSystemName() != null) recordAsObjects[0] = record.getTag().getSystemName();
                if (record.getTag().getTagProvider() != null) recordAsObjects[1] = record.getTag().getTagProvider();
                if (record.getTag().getTagPath() != null) recordAsObjects[2] = record.getTag().getTagPath();
                Object value = record.getValue();
                if (value != null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    String valueAsJson = objectMapper.writeValueAsString(value);
                    recordAsObjects[3] = valueAsJson;

                    if (value instanceof Double || value instanceof Float) {
                        recordAsObjects[4] = value;
                    } else if (value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                        recordAsObjects[4] = value;
                        recordAsObjects[5] = value;
                    }
                }

                if (record.getTimestamp() != null) {
                    String formattedDate = simpleDateFormat.format(record.getTimestamp());
                    recordAsObjects[6] = formattedDate;
                }
                if (record.getQuality() != null) recordAsObjects[7] = record.getQuality();
                csvWriter.writeRow(recordAsObjects);
            }
        }
        csvWriter.flush();
        gzipOutputStream.finish();
        gzipOutputStream.close();
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(new ByteArrayInputStream(bis.toByteArray()), false);
        streamSourceInfo.setCompressionType(CompressionType.gz);
        gzipOutputStream.finish();
        gzipOutputStream.close();
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
