package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.Task;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Is used to show live data in the UI.
 */
public class LiveState {

    private final Map<SlaveID, Task> tasks;

    public LiveState() {
        tasks = Collections.synchronizedMap(new HashMap<>());
    }

    public Set<Task> getNonTerminalTasks() {
        return tasks.entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .collect(toSet());
    }

    public void removeTask(SlaveID slaveId) {
        tasks.remove(slaveId);
    }

    public void updateTaskStatus(Protos.TaskStatus status, Protos.TaskInfo taskInfo) {
        if (!StateUtil.isTerminalState(status.getState())) {

            tasks.put(status.getSlaveId(), new Task(status.getTaskId(), taskInfo.getSlaveId(),
                taskInfo.getExecutor().getExecutorId()));
        } else {
            removeTask(status.getSlaveId());
        }

    }

    public void updateStats(SlaveID slaveID, ExecutorMessage messages) {
        tasks.put(slaveID,
            new Task(tasks.get(slaveID), messages.getContainersList(), messages.getStatus()));
    }
}

