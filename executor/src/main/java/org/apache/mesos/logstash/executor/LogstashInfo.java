package org.apache.mesos.logstash.executor;

/**
 * Created by ero on 15/06/15.
 */
public final class LogstashInfo {
    private final String confguration;
    private final String name;

    public String getConfiguration() {
        return this.confguration;
    }

    public String getName() {
        return name;
    }

    public LogstashInfo(String name, String configuration) {
        this.name = name;
        this.confguration = configuration;
    }
}
