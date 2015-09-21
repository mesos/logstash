package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorStatus;

import java.util.Collections;
import java.util.List;

public class Task {

    private final Protos.TaskID taskId;
    private final Protos.SlaveID slaveID;
    private final Protos.ExecutorID executorID;
    private final List<ContainerState> containers;

    private final ExecutorStatus status;

    private Protos.TaskState state = null;
    private String hostName;

    public Task(Protos.TaskID taskId, Protos.SlaveID slaveID, Protos.ExecutorID executorID) {
        this.taskId = taskId;
        this.slaveID = slaveID;
        this.executorID = executorID;
        this.containers = Collections.emptyList();
        this.status = ExecutorStatus.INITIALIZING;
        this.hostName = "unknown";
    }

    public Task(Task other, List<ContainerState> containers, ExecutorStatus status, String hostName) {
        this.containers = containers;
        this.taskId = other.taskId;
        this.slaveID = other.slaveID;
        this.executorID = other.executorID;
        this.hostName = hostName;
        this.status = status;
    }

    public Protos.SlaveID getSlaveID() {
        return slaveID;
    }

    public Protos.ExecutorID getExecutorID() {
        return executorID;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 7). // two randomly chosen prime numbers
            append(slaveID.getValue()).
            toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Task)) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        Task rhs = (Task) obj;
        return new EqualsBuilder().
            append(slaveID.getValue(), rhs.slaveID.getValue()).
            isEquals();
    }

    public Protos.TaskState getTaskState() {
        return state;
    }

    public ExecutorStatus getExecutorStatus() {
        return status;
    }

    public void setStatus(Protos.TaskState status) {
        this.state = status;
    }

    public long getActiveStreamCount() {
        return this.containers.stream().filter(
            c -> c.getType().equals(ContainerState.LoggingStateType.STREAMING)).count();
    }

    public Protos.TaskID getTaskId() {
        return taskId;
    }

    public List<ContainerState> getContainers() {
        return Collections.unmodifiableList(containers);
    }

    public String getHostName() {
        return hostName;
    }
}
