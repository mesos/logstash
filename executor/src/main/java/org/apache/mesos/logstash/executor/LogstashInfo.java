package org.apache.mesos.logstash.executor;

/**
 * Created by ero on 15/06/15.
 */
public final class LogstashInfo {
    private final String loggingLocationPath;
    private final String confgurationPath;

    public String GetLoggingLocationPath() {
        return this.loggingLocationPath;
    }

    public String GetConfigurationPath() {
        return this.confgurationPath;
    }

    public LogstashInfo(String loggingLocationPath, String configurationPath) {
        this.loggingLocationPath = loggingLocationPath;
        this.confgurationPath = configurationPath;
    }
}
