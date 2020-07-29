package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.StatMetric;
import com.inductiveautomation.ignition.common.i18n.LocalizedString;
import com.inductiveautomation.ignition.gateway.history.*;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.microsoft.opensource.cla.ignition.TagValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Responsible for actually storing the data to ADX. Can either use the
 * built-in store & forward system for Ignition or use its own.
 */
public class AzureKustoHistorySink implements DataSink, StoreAndForwardEngine {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private GatewayContext context;
    private String pipelineName;

    public AzureKustoHistorySink(String pipelineName, GatewayContext context) {
        this.pipelineName = pipelineName;
        this.context = context;
    }

    @Override
    public String getPipelineName() {
        return pipelineName;
    }

    @Override
    public void startup() {

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
    public void storeData(HistoricalData data) {
        logger.debug("Received data of type '" + data.getClass().toString() + "'");

        List<TagValue> records = new ArrayList<TagValue>();

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
                    TagValue tagValue = new TagValue(context, historicalTagValue);
                    logger.trace(tagValue.toString());
                    records.add(tagValue);
                }
            } else if (d instanceof HistoricalTagValue) {
                HistoricalTagValue dValue = (HistoricalTagValue) d;
                TagValue tagValue = new TagValue(context, dValue);
                logger.trace(tagValue.toString());
                records.add(tagValue);
            }
        }

        if (records.size() > 0) {
            logger.debug("Logging " + records.size() + " records");
            // TODO: Store records to ADX - // Ohad
        }
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
