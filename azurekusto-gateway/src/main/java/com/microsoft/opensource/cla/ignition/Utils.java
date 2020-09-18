package com.microsoft.opensource.cla.ignition;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    public static String getDMUriFromSetting(String clusterURL) {
        if (clusterURL.startsWith("http")) {
            int index = clusterURL.startsWith("https") ? "https://".length() : "http://".length();
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

    public static String getDateLiteral(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS");

        return "datetime(" + sdf.format(d) + ")";
    }
}
