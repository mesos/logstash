package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.Task;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

import static java.util.stream.Collectors.toSet;

public class LiveState implements ILiveState {

    private final Map<SlaveID, Task> tasks;
    private final Set<SlaveID> stagingTasks;

    @Autowired
    public LiveState() {
        tasks = Collections.synchronizedMap(new HashMap<>());
        stagingTasks = Collections.synchronizedSet(new HashSet<>());
    }

    @Override
    public Set<Task> getRunningTasks() {
        return tasks.entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .collect(toSet());
    }

    @Override public boolean isAlreadyStaging(SlaveID slaveID){
        return stagingTasks.contains(slaveID);
    }


    @Override
    public void removeTask(SlaveID slaveId) {
        tasks.remove(slaveId);
        stagingTasks.remove(slaveId);
    }

    @Override public void addRunningTask(Task task) {
        tasks.put(task.getSlaveID(), task);
        stagingTasks.remove(task.getSlaveID());
    }

    @Override
    public void updateStats(SlaveID slaveID, ExecutorMessage messages) {
        tasks.put(slaveID, new Task(tasks.get(slaveID), messages.getContainersList(), messages.getStatus()));
    }

    @Override public void addStagingTaskOnSlave(SlaveID slaveId) {
        stagingTasks.add(slaveId);
    }
}
