package com.microsoft.opensource.cla.ignition;

import com.inductiveautomation.ignition.common.BundleUtil;
import com.inductiveautomation.ignition.common.licensing.LicenseState;
import com.inductiveautomation.ignition.gateway.model.AbstractGatewayModuleHook;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.microsoft.opensource.cla.ignition.azurekusto.AzureKustoHistoryProvider;
import com.microsoft.opensource.cla.ignition.azurekusto.AzureKustoHistoryProviderType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The GatewayHook is the entry for the module. The hook is responsible for
 * using extension points and adding profiles to Ignition. This hook adds a
 * new tag history provider type that a developer can use to store/retrieve
 * data from Azure Data Explorer.
 */
public class GatewayHook extends AbstractGatewayModuleHook {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private GatewayContext context;
    private AzureKustoHistoryProviderType azureKustoHistoryProviderType;

    @Override
    public void setup(GatewayContext gatewayContext) {
        this.context = gatewayContext;

        azureKustoHistoryProviderType = new AzureKustoHistoryProviderType();

        // Add bundle resource for localization
        BundleUtil.get().addBundle(AzureKustoHistoryProvider.class);

        // Add Azure Kusto history provider type
        try {
            context.getTagManager().addSQLTagHistoryProviderType(azureKustoHistoryProviderType);
        } catch (Exception ex) {
            logger.error("Error adding Azure Kusto history provider type", ex);
        }
    }

    @Override
    public void startup(LicenseState licenseState) {

    }

    @Override
    public void shutdown() {
        // Remove bundle resource
        BundleUtil.get().removeBundle(AzureKustoHistoryProviderType.class);

        // Remove Azure Kusto history provider type
        try {
            context.getTagManager().removeSQLTagHistoryProviderType(azureKustoHistoryProviderType);
        } catch (Exception ex) {
            logger.error("Error shutting down Azure Kusto history provider type", ex);
        }
    }

}
