package com.microsoft.opensource.cla.ignition.azurekusto;

import com.inductiveautomation.ignition.gateway.localdb.persistence.*;
import com.inductiveautomation.ignition.gateway.sqltags.config.SQLTagHistoryProviderRecord;
import simpleorm.dataset.SFieldFlags;

/**
 * Represents the settings required for the Azure Kusto history provider.
 * A user will fill in all of these fields. The data is stored inside of
 * Ignition's internal sqlite database into the table specified below.
 */
public class AzureKustoHistoryProviderSettings extends PersistentRecord {
    public static final RecordMeta<AzureKustoHistoryProviderSettings> META = new RecordMeta<AzureKustoHistoryProviderSettings>(
            AzureKustoHistoryProviderSettings.class, "AzureKustoHistoryProviderSettings");

    public static final LongField ProfileId = new LongField(META, "ProfileId", SFieldFlags.SPRIMARY_KEY);
    public static final ReferenceField<SQLTagHistoryProviderRecord> Profile =
            new ReferenceField<SQLTagHistoryProviderRecord>(META, SQLTagHistoryProviderRecord.META, "Profile", ProfileId);

    public static final StringField ApplicationId = new StringField(META, "ApplicationId", SFieldFlags.SMANDATORY);
    public static final StringField AADTenantId = new StringField(META, "AADTenantId", SFieldFlags.SMANDATORY);
    public static final EncodedStringField ApplicationKey = new EncodedStringField(META, "ApplicationKey", SFieldFlags.SMANDATORY);
    public static final StringField ClusterURL = new StringField(META, "ClusterURL", SFieldFlags.SMANDATORY).setDefault("https://ignitionadxpoc.eastus.kusto.windows.net");
    public static final StringField DatabaseName = new StringField(META, "DatabaseName", SFieldFlags.SMANDATORY);

    static {
        ProfileId.getFormMeta().setVisible(false);
        Profile.getFormMeta().setVisible(false);
    }

    @Override
    public RecordMeta<?> getMeta() {
        return META;
    }
}
