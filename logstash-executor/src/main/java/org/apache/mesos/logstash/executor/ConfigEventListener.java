package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogType;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;

import java.util.stream.Stream;

public interface ConfigEventListener {
    void onConfigUpdated(LogType type, Stream<FrameworkInfo> frameworks);
}
