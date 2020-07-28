package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.gateway.localdb.persistence.PersistentRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.RecordMeta;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.sqltags.config.SQLTagHistoryProviderRecord;
import com.inductiveautomation.ignition.gateway.sqltags.config.SQLTagHistoryProviderType;
import com.inductiveautomation.ignition.gateway.sqltags.history.TagHistoryProvider;

/**
 * Describes the Azure Kusto history provider. Each provider has a unique id
 * (TYPE_ID), points to the settings record, and instantiates an instance of
 * the provider with the proper settings.
 */
public class AzureKustoHistoryProviderType extends SQLTagHistoryProviderType {
    public static final String TYPE_ID = "AzureKusto";

    public AzureKustoHistoryProviderType() {
        super(TYPE_ID, "HistoryProvider.ProviderType.Name", "HistoryProvider.ProviderType.Desc");
    }

    @Override
    public RecordMeta<? extends PersistentRecord> getSettingsRecordType() {
        return AzureKustoHistoryProviderSettings.META;
    }

    /**
     * Creates a new instance of the Azure Kusto history provider for storage and retrieval
     */
    @Override
    public TagHistoryProvider createHistoryProvider(SQLTagHistoryProviderRecord profile, GatewayContext context)
            throws Exception {
        AzureKustoHistoryProviderSettings settings = findProfileSettingsRecord(context, profile);
        AzureKustoHistoryProvider ret = new AzureKustoHistoryProvider(context, profile.getName(), settings);
        return ret;
    }
}
