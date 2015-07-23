package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.Task;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

import static java.util.stream.Collectors.toSet;

public class LogstashLiveState implements LiveState {

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

    @Override
    public void updateStats(Protos.SlaveID slaveID, ExecutorMessage messages) {
        tasks.put(slaveID, new Task(tasks.get(slaveID), messages.getContainersList(), messages.getStatus()));
    }
}
