package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.model.types.DataQuality;
import com.inductiveautomation.ignition.gateway.history.HistoricalTagValue;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;

import java.util.Date;

/**
 * Represents a historian tag value. The tag is qualified by:
 *    systemName - The name of the Ignition Gateway the tag is from
 *    tagProvider - The name of the tag database the tag is from
 *    tagPath - The fully qualified tag path, path/to/my/tag
 *    value - The value of the tag
 *    timestamp - The timestamp for the value
 *    quality - A quality code that represents the quality of the value, Good=192 else Bad
 */
public class AzureKustoTagValue {
    private AzureKustoTag tag;
    private Object value;
    private Date timestamp;
    private Integer quality;

    public AzureKustoTagValue(AzureKustoTag tag, Object value, Date timestamp, Integer quality){
        this.tag = tag;
        this.value = value;
        this.timestamp = timestamp;
        this.quality = quality;
    }

    public AzureKustoTagValue(GatewayContext context, HistoricalTagValue historicalTagValue) {
        // Pull all of the required fields from the historical tag value
        String systemName = (historicalTagValue == null || historicalTagValue.getSource().getSystem() == null) ? context.getSystemProperties().getSystemName() : historicalTagValue.getSource().getSystem();
        String tagProvider = historicalTagValue.getSource().getSource();
        String tagPath = historicalTagValue.getSource().toStringPartial();
        this.tag = new AzureKustoTag(systemName, tagProvider, tagPath);
        this.value = historicalTagValue.getValue();
        this.timestamp = historicalTagValue.getTimestamp();
        this.quality = ((DataQuality) historicalTagValue.getQuality()).getIntValue();
    }

    public AzureKustoTag getTag() {
        return tag;
    }

    public void setTag(AzureKustoTag tag) {
        this.tag = tag;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getQuality() {
        return quality;
    }

    public void setQuality(Integer quality) {
        this.quality = quality;
    }

    @Override
    public String toString() {
        return "AzureKustoTagValue [path=" + tag.toStringFull() + ", value=" + value + ", timestamp=" + timestamp + ", quality=" + quality + "]";
    }
}
