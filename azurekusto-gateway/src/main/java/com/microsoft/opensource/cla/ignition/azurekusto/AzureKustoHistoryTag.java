package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryNode;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ProcessedHistoryColumn;

/**
 * Represents a historical tag to query in ADX. Combines the fully qualified
 * tag path, aggregation function, and return tag.
 */
public class AzureKustoHistoryTag {
    private String systemName;
    private String tagProvider;
    private String tagPath;
    private Aggregate aggregate;
    private HistoryNode tag;

    public AzureKustoHistoryTag(AzureKustoTagValue tagValue, Aggregate aggregate, HistoryNode tag) {
        this.systemName = tagValue.getSystemName();
        this.tagProvider = tagValue.getTagProvider();
        this.tagPath = tagValue.getTagPath();
        this.aggregate = aggregate;
        this.tag = tag;
    }

    public String getSystemName() {
        return systemName;
    }

    public String getTagProvider() {
        return tagProvider;
    }

    public String getTagPath() {
        return tagPath;
    }

    public Aggregate getAggregate() {
        return aggregate;
    }

    public HistoryNode getTag() {
        return tag;
    }

    public ProcessedHistoryColumn getProcessedHistoryTag() {
        return (ProcessedHistoryColumn) tag;
    }

    /**
     * Valid only for ProcessedHistoryColumn type since the tag path is valid.
     */
    public boolean valid() {
        return tag instanceof ProcessedHistoryColumn;
    }
}
