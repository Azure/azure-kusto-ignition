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
    private String systemName;
    private String tagProvider;
    private String tagPath;
    private Object value;
    private Date timestamp;
    private Integer quality;

    public AzureKustoTagValue() {

    }

    public AzureKustoTagValue(GatewayContext context, HistoricalTagValue historicalTagValue) {
        // Pull all of the required fields from the historical tag value
        this.systemName = (historicalTagValue == null || historicalTagValue.getSource().getSystem() == null) ? context.getSystemProperties().getSystemName() : historicalTagValue.getSource().getSystem();
        this.tagProvider = historicalTagValue.getSource().getSource();
        this.tagPath = historicalTagValue.getSource().toStringPartial();
        this.value = historicalTagValue.getValue();
        this.timestamp = historicalTagValue.getTimestamp();
        this.quality = ((DataQuality) historicalTagValue.getQuality()).getIntValue();
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getTagProvider() {
        return tagProvider;
    }

    public void setTagProvider(String tagProvider) {
        this.tagProvider = tagProvider;
    }

    public String getTagPath() {
        return tagPath;
    }

    public void setTagPath(String tagPath) {
        this.tagPath = tagPath;
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

    /**
     * Return the fully qualified tag path including system, tag provider, and tag path
     */
    public String toStringFull() {
        return "[" + systemName + ";" + tagProvider + "]" + tagPath;
    }

    @Override
    public String toString() {
        return "TagValue [path=" + toStringFull() + ", value=" + value + ", timestamp=" + timestamp + ", quality=" + quality + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AzureKustoTagValue) {
            AzureKustoTagValue trObj = (AzureKustoTagValue) obj;
            return toStringFull().equals(trObj.toStringFull());
        }

        return false;
    }
}
