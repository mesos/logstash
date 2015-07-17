package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;

public interface FrameworkMessageListener {
    void frameworkMessage(ExecutorInfo executor, ExecutorMessage message);
}
