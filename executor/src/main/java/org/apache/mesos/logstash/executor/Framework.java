package org.apache.mesos.logstash.executor;

import java.util.Map;

/**
 * Created by ero on 22/06/15.
 */
public class Framework {
    private String localLogLocation;

    public String getId() {
        return id;
    }

    // Get the log location INSIDE THE CONTAINER RUNNING THE SERVICE
    public String getLogLocation() {
        return logLocation;
    }

    // Get the log location INSIDE THE LOGSTASH EXECUTOR's CONTAINER
    public String getLocalLogLocations() { return localLogLocation; }

    private String id;
    private String logLocation;
    private String logType;

    public Framework(Map.Entry<String, LogstashInfo> entry) {
        this.logType = entry.getValue().getName();
        this.id = entry.getKey();
        this.logLocation = entry.getValue().getLoggingLocationPath();
    }

    public void setLocalLogLocation(String localLogLocation) {
        this.localLogLocation = localLogLocation;
    }
}
