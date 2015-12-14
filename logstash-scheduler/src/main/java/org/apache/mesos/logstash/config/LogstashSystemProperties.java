package org.apache.mesos.logstash.config;

import org.apache.mesos.logstash.state.SerializableState;

import java.util.Optional;
import java.util.Properties;

@Deprecated
public class LogstashSystemProperties {

    private static final int DEFAULT_FAILOVER_TIMEOUT = 31449600;

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

    public LogstashSystemProperties() {
        this.props = System.getProperties();
    }

    public String getNativeLibrary() {
        return props.getProperty("mesos.native.library", null);
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

    public String getVolumes() { return props.getProperty("mesos.logstash.volumes", ""); }

    public boolean isDisableFailover() {
        return getBoolean("mesos.logstash.disableFailover", false);
    }
}
