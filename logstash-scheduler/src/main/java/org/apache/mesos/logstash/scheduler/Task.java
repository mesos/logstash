package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.mesos.Protos;

public class Task {

    private final Protos.TaskID taskId;
    private final Protos.SlaveID slaveID;
    private final Protos.ExecutorID executorID;

    private Protos.TaskState status = null;

    private int activeStreamCount = 0;

    public Task(Protos.TaskID taskId, Protos.SlaveID slaveID, Protos.ExecutorID executorID) {
        this.taskId = taskId;
        this.slaveID = slaveID;
        this.executorID = executorID;
    }

    public int getActiveStreamCount() {
        return activeStreamCount;
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
        if (!(obj instanceof Task))
            return false;
        if (obj == this)
            return true;

        Task rhs = (Task) obj;
        return new EqualsBuilder().
            append(slaveID.getValue(), rhs.slaveID.getValue()).
            isEquals();
    }

    public Protos.TaskState getStatus() {
        return status;
    }

    public void setStatus(Protos.TaskState status) {
        this.status = status;
    }

    public void setActiveStreamCount(int activeStreamCount) {
        this.activeStreamCount = activeStreamCount;
    }

    public Protos.TaskID getTaskId() {
        return taskId;
    }
}
