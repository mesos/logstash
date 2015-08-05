package org.apache.mesos.logstash.config;

import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class LogstashSettings {

    private static final int DEFAULT_LOGSTASH_HEAP_SIZE = 512;
    private static final int DEFAULT_EXECUTOR_HEAP_SIZE = 256;

    private static final double DEFAULT_CPUS = 0.1;
    private static final double DEFAULT_EXECUTOR_CPUS = DEFAULT_CPUS;

    private static final int DEFAULT_FAILOVER_TIMEOUT = 31449600;
    private static final int DEFAULT_ZK_TIME_MS = 20000;

    private static final int DEFAULT_WEB_SERVER_PORT = 9092;

    private final Properties props;

    private int getInt(String key, int defaultValue) {
        String value = props.getProperty(key);
        return (value != null) ? Integer.valueOf(value, 10) : defaultValue;
    }

    private double getDouble(String key, double defaultValue) {
        String value = props.getProperty(key);
        return (value != null) ? Double.valueOf(value) : defaultValue;
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        String value = props.getProperty(key);
        return (value != null) ? Boolean.valueOf(value) : defaultValue;
    }

    private long getLong(String key, long defaultValue) {
        String value = props.getProperty(key);
        return (value != null) ? Long.valueOf(value) :defaultValue;
    }

    public LogstashSettings() {
        this.props = System.getProperties();
    }

    public String getNativeLibrary() {
        return props.getProperty("mesos.native.library", null);
    }

    public double getExecutorCpus() {
        return getDouble("mesos.logstash.executor.cpus", DEFAULT_EXECUTOR_CPUS);
    }

    public int getExecutorHeapSize() {
        return getInt("mesos.logstash.executor.heap.size", DEFAULT_EXECUTOR_HEAP_SIZE);
    }

    public int getLogstashHeapSize() {
        return getInt("mesos.logstash.logstash.heap.size", DEFAULT_LOGSTASH_HEAP_SIZE);
    }

    public String getMesosMasterUri() {
        return props.getProperty("mesos.master.uri", "zk://localhost:2181/mesos");
    }

    public String getFrameworkName() {
        return props.getProperty("mesos.logstash.framework.name", "logstash");
    }

    public long getFailoverTimeout() {
        return getLong("mesos.failover.timeout.sec", DEFAULT_FAILOVER_TIMEOUT);
    }

    public boolean getWebServerEnabled() {
        return getBoolean("mesos.logstash.web.enabled", true);
    }

    public int getWebServerPort() {
        return getInt("mesos.logstash.web.port", DEFAULT_WEB_SERVER_PORT);
    }

    public String getLogstashUser() {
        return props.getProperty("mesos.logstash.user", "root");
    }

    public String getLogstashRole() {
        return props.getProperty("mesos.logstash.role", "*");
    }

    public String getStateZkServers() {
        return props.getProperty("mesos.logstash.state.zk", "localhost:2181");
    }

    public int getStateZkTimeout() {
        return getInt("mesos.logstash.state.zk.timeout.ms", DEFAULT_ZK_TIME_MS);
    }
}
