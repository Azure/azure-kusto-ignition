package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.common.sqltags.history.Aggregate;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryNode;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.columns.ProcessedHistoryColumn;

/**
 * Represents a historical tag to query in ADX. Combines the fully qualified
 * tag path, aggregation function, and return tag.
 */
public class AzureKustoHistoryTag {
    private AzureKustoTag tag;
    private Aggregate aggregate;
    private HistoryNode historyTag;

    public AzureKustoHistoryTag(AzureKustoTag tag, Aggregate aggregate, HistoryNode historyTag) {
        this.tag = tag;
        this.aggregate = aggregate;
        this.historyTag = historyTag;
    }

    public AzureKustoTag getTag() {
        return tag;
    }

    public Aggregate getAggregate() {
        return aggregate;
    }

    public HistoryNode getHistoryTag() {
        return historyTag;
    }

    public ProcessedHistoryColumn getProcessedHistoryTag() {
        return (ProcessedHistoryColumn) historyTag;
    }

    /**
     * Valid only for ProcessedHistoryColumn type since the tag path is valid.
     */
    public boolean valid() {
        return historyTag instanceof ProcessedHistoryColumn;
    }
}
