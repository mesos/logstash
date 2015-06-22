package org.apache.mesos.logstash.executor;

import java.util.Map;

/**
 * Created by ero on 22/06/15.
 */
public class Framework {
    public String getId() {
        return id;
    }

    public String getLogstashFilter() {
        return logstashFilter;
    }

    public String getLogLocation() {
        return logLocation;
    }

    private String id;
    private String logstashFilter;
    private String logLocation;

    public Framework(String id, String logstashFilter, String logLocation) {
        this.id = id;
        this.logstashFilter = logstashFilter;
        this.logLocation = logLocation;
    }

    public Framework(Map.Entry<String, LogstashInfo> entry) {
        this.id = entry.getKey();
        this.logstashFilter = entry.getValue().getConfiguration();
        this.logLocation = entry.getValue().getLoggingLocationPath();
    }
}
