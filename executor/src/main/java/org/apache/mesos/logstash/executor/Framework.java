package org.apache.mesos.logstash.executor;

import java.util.Map;

/**
 * Created by ero on 22/06/15.
 */
public class Framework {
    private String localLogLocation;
    private String logType;

    public String getId() {
        return id;
    }

    public String getLogType() { return logType; }
    public String[] getLogTags() { return new String[]{"dummyLogTag"}; }

    public String getLogstashFilter() {
        return logstashFilter;
    }

    // Get the log location INSIDE THE CONTAINER RUNNING THE SERVICE
    public String getLogLocation() {
        return logLocation;
    }

    // Get the log location INSIDE THE LOGSTASH EXECUTOR's CONTAINER
    public String getLocalLogLocation() { return localLogLocation; }

    private String id;
    private String logstashFilter;
    private String logLocation;

    private String configBlock; // for filtering/parsing log messages

    public Framework(String id, String logstashFilter, String logLocation) {
        this.id = id;
        this.logstashFilter = logstashFilter;
        this.logLocation = logLocation;
    }

    public Framework(Map.Entry<String, LogstashInfo> entry) {
        this.logType = entry.getValue().getName();
        this.id = entry.getKey();
        this.logstashFilter = entry.getValue().getConfiguration();
        this.logLocation = entry.getValue().getLoggingLocationPath();
    }

    public boolean hasFilterSection() { return false; } // TODO

    public String getFilterSection() { return ""; } // TODO

    public void setLocalLogLocation(String localLogLocation) {
        this.localLogLocation = localLogLocation;
    }
}
