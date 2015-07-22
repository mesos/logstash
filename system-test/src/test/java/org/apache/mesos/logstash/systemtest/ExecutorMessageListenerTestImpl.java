package org.apache.mesos.logstash.systemtest;

import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.Task;
import org.apache.mesos.logstash.scheduler.FrameworkMessageListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class ExecutorMessageListenerTestImpl implements FrameworkMessageListener {
    List<ExecutorMessage> messages = Collections.synchronizedList(new ArrayList<ExecutorMessage>());

    public synchronized List<ExecutorMessage> getExecutorMessages(){
        return messages;
    }

    public synchronized void clearAllMessages(){
        messages.clear();
    }

    @Override
    public void frameworkMessage(Task executor, ExecutorMessage message) {
        messages.add(message);
    }
}
