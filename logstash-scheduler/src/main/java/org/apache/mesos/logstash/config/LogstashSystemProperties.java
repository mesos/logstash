package org.apache.mesos.logstash.config;

import org.apache.mesos.logstash.state.SerializableState;

import java.util.Optional;
import java.util.Properties;

@Deprecated
public class LogstashSystemProperties {

    private final Properties props;

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

    public boolean isDisableFailover() {
        return getBoolean("mesos.logstash.disableFailover", false);
    }
}
