package org.apache.mesos.logstash.scheduler.ui;


import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.mesos.Protos;

public class Executor {

    private final Protos.SlaveID slaveID;
    private final Protos.ExecutorID executorID;

    public Executor(Protos.SlaveID slaveID, Protos.ExecutorID executorID) {
        this.slaveID = slaveID;
        this.executorID = executorID;
    }

    public Protos.SlaveID getSlaveID() {
        return slaveID;
    }

    public Protos.ExecutorID getExecutorID() {
        return executorID;
    }

    public static Executor fromTaskStatus(Protos.TaskStatus status) {
        return new Executor(status.getSlaveId(), status.getExecutorId());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 7). // two randomly chosen prime numbers
                append(executorID).
                append(slaveID).
                toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Executor))
            return false;
        if (obj == this)
            return true;

        Executor rhs = (Executor) obj;
        return new EqualsBuilder().
                append(executorID, rhs.executorID).
                append(slaveID, rhs.slaveID).
                isEquals();
    }
}
