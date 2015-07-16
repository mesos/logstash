package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.ui.Executor;

public interface FrameworkMessageListener {
    void frameworkMessage(Executor executor, ExecutorMessage message);
}
