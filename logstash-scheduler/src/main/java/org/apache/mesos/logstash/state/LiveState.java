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

public class LiveState {

    private final Map<SlaveID, Task> tasks;

    public LiveState() {
        tasks = Collections.synchronizedMap(new HashMap<>());
    }

    public Set<Task> getRunningTasks() {
        return tasks.entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .collect(toSet());
    }


    public void removeTask(SlaveID slaveId) {
        tasks.remove(slaveId);
    }

    public void updateTaskStatus(Protos.TaskStatus status) {
        if (status.getState().equals(Protos.TaskState.TASK_RUNNING)){
            tasks.put(status.getSlaveId(), new Task(status.getTaskId(), status.getSlaveId(), status.getExecutorId()));
        } else {
            removeTask(status.getSlaveId());
        }

    }

    public void updateStats(SlaveID slaveID, ExecutorMessage messages) {
        tasks.put(slaveID, new Task(tasks.get(slaveID), messages.getContainersList(), messages.getStatus()));
    }

}
