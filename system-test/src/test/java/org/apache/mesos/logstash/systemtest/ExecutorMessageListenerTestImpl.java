package org.apache.mesos.logstash.systemtest;

import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.ExecutorMessageListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class ExecutorMessageListenerTestImpl implements ExecutorMessageListener {
    List<ExecutorMessage> messages = Collections.synchronizedList(new ArrayList<ExecutorMessage>());

    @Override
    public synchronized void onNewMessageReceived(ExecutorMessage message) {
        messages.add(message);
    }


    public synchronized List<ExecutorMessage> getExecutorMessages(){
        return messages;
    }

    public synchronized void clearAllMessages(){
        messages.clear();
    }
}
