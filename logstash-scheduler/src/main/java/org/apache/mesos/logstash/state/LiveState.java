package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.scheduler.Task;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public interface LiveState {

    Set<Task> getTasks();

    void removeRunningTask(Protos.SlaveID slaveId);

    void addRunningTask(Task task);

    void updateStats(Protos.SlaveID slaveID, List<LogstashProtos.ContainerState> containers);
}
