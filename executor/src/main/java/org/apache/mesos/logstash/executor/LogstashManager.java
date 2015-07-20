package org.apache.mesos.logstash.executor;

public interface LogstashManager {
    void updateConfig(LogType type, String config);
}
