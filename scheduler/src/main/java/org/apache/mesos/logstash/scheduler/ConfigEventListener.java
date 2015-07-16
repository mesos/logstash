package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.scheduler.ConfigManager.ConfigPair;

public interface ConfigEventListener {
    void configUpdated(ConfigPair configPair);
}
