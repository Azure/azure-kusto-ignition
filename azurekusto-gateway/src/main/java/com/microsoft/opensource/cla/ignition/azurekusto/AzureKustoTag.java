package com.microsoft.opensource.cla.ignition.azurekusto;

import java.util.Objects;

public class AzureKustoTag {
    private String systemName;
    private String tagProvider;
    private String tagPath;

    public AzureKustoTag(String systemName, String tagProvider, String tagPath) {
        this.systemName = systemName;
        this.tagProvider = tagProvider;
        this.tagPath = tagPath;
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

    /**
     * Return the fully qualified tag path including system, tag provider, and tag path
     */
    public String toStringFull() {
        return "[" + systemName + ";" + tagProvider + "]" + tagPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureKustoTag that = (AzureKustoTag) o;
        return systemName.equalsIgnoreCase(that.systemName) &&
                tagProvider.equalsIgnoreCase(that.tagProvider) &&
                tagPath.equalsIgnoreCase(that.tagPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemName.toLowerCase(), tagProvider.toLowerCase(), tagPath.toLowerCase());
    }
}
