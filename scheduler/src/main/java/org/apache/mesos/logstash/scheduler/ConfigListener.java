package org.apache.mesos.logstash.scheduler;

import java.util.Map;

public interface ConfigListener {
    void onNewConfig(ConfigMonitor monitor, Map<String, String> configs);
}
