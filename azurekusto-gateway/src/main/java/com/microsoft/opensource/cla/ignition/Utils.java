package com.microsoft.opensource.cla.ignition;

public class Utils {
    public static String getDMUriFromSetting(String clusterURL) {
        if(clusterURL.startsWith("http")){
            int index = clusterURL.startsWith("https")? "https://".length() : "http://".length();
            return clusterURL.substring(0, index) + "ingest-" + clusterURL.substring(index);
        }
        return String.format("https://ingest-%s.kusto.windows.net", clusterURL);
    }

    public static String getEngineUriFromSetting(String clusterURL) {
        if (clusterURL.startsWith("http")) {
            return clusterURL;
        }
        return String.format("https://%s.kusto.windows.net", clusterURL);
    }
}
