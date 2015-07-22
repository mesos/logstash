package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.scheduler.FrameworkMessageListener;
import org.apache.mesos.logstash.scheduler.LogstashScheduler;
import org.apache.mesos.logstash.scheduler.Task;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class LogstashLiveState implements LiveState, FrameworkMessageListener {

    private final Map<Protos.SlaveID, Task> tasks;

    @Autowired
    public LogstashLiveState() {
        tasks = Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    public Set<Task> getTasks() {
        return tasks.entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .collect(toSet());
    }

    @Override
    public void removeRunningTask(Protos.SlaveID slaveId) {
        tasks.remove(slaveId);
    }

    @Override public void addRunningTask(Task task) {
        tasks.put(task.getSlaveID(), task);
    }

    @Override public void frameworkMessage(LogstashScheduler scheduler,
        Protos.ExecutorID executorID, Protos.SlaveID slaveID,
        LogstashProtos.ExecutorMessage message) {

    }
}
