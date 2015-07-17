package org.apache.mesos.logstash.scheduler;


import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.mesos.Protos;

public class ExecutorInfo {

    private final Protos.SlaveID slaveID;
    private final Protos.ExecutorID executorID;

    public ExecutorInfo(Protos.SlaveID slaveID, Protos.ExecutorID executorID) {
        this.slaveID = slaveID;
        this.executorID = executorID;
    }

    public Protos.SlaveID getSlaveID() {
        return slaveID;
    }

    public Protos.ExecutorID getExecutorID() {
        return executorID;
    }

    public static ExecutorInfo fromTaskStatus(Protos.TaskStatus status) {
        return new ExecutorInfo(status.getSlaveId(), status.getExecutorId());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 7). // two randomly chosen prime numbers
                append(executorID.getValue()).
                append(slaveID.getValue()).
                toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ExecutorInfo))
            return false;
        if (obj == this)
            return true;

        ExecutorInfo rhs = (ExecutorInfo) obj;
        return new EqualsBuilder().
                append(executorID.getValue(), rhs.executorID.getValue()).
                append(slaveID.getValue(), rhs.slaveID.getValue()).
                isEquals();
    }
}
