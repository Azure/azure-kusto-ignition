package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryNode;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ProcessedHistoryColumn;

/**
 * Represents a historical tag to query in ADX. Combines the fully qualified
 * tag path, aggregation function, and return tag.
 */
public class AzureKustoHistoryTag {
    private String tagPath;
    private Aggregate aggregate;
    private HistoryNode tag;

    public AzureKustoHistoryTag(String tagPath, Aggregate aggregate, HistoryNode tag) {
        this.tagPath = tagPath;
        this.aggregate = aggregate;
        this.tag = tag;
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
