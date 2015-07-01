package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos;

import java.io.File;
import java.nio.file.Paths;

/**
 * Created by ero on 30/06/15.
 */
public class LogConfiguration {
    // Get the log location INSIDE THE CONTAINER RUNNING THE SERVICE
    public String getLocalLogLocation() {
        return localLogLocation;
    }

    public String getLogLocation() {
        return logLocation;
    }

    public String getLogType() {
        return logType;
    }

    public String getTag() {
        return tag;
    }

    public void setLocalLogLocation(String frameworkName, String localLogLocation) {
        File fileToLog = new File(this.logLocation);
        String sanitizedFrameworkName = sanitize(frameworkName);
        this.localLogLocation = Paths.get(localLogLocation, sanitizedFrameworkName, fileToLog.getName()).toString();
    }

    private String sanitize(String frameworkName) {
        return frameworkName.replaceFirst(".*/", "").replaceFirst(":\\w+", "");
    }

    private final String logLocation;
    private final String logType;
    private final String tag;
    private String localLogLocation;

    public LogConfiguration(LogstashProtos.LogInputConfiguration logInputConfiguration) {
        this(logInputConfiguration.getType(), logInputConfiguration.getLocation(), logInputConfiguration.getTag());
    }

    public LogConfiguration(String logType, String logLocation, String tag) {
        this.logType = logType;
        this.logLocation = logLocation;
        this.tag = tag;
    }
}
