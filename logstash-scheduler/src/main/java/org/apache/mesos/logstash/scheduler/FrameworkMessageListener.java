package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;

public interface FrameworkMessageListener {
    void frameworkMessage(LogstashScheduler scheduler, Protos.ExecutorID executorID,
        Protos.SlaveID slaveID, ExecutorMessage message);
}
