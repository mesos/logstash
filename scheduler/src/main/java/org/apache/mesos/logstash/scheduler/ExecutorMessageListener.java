package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;

public interface ExecutorMessageListener {
    void onNewMessageReceived(ExecutorMessage message);
}
