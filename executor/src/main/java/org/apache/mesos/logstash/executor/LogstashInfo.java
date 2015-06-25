package org.apache.mesos.logstash.executor;

/**
 * Created by ero on 15/06/15.
 */
public final class LogstashInfo {
    private final String loggingLocationPath;
    private final String confguration;
    private final String name;

    public String getLoggingLocationPath() {
        return this.loggingLocationPath;
    }

    public String getConfiguration() {
        return this.confguration;
    }

    public LogstashInfo(String name, String loggingLocationPath, String configuration) {
        this.name = name;
        this.loggingLocationPath = loggingLocationPath;
        this.confguration = configuration;
    }

    public String getName() {
        return name;
    }
}
