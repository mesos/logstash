package org.apache.mesos.logstash.scheduler.ui;

import org.apache.mesos.logstash.scheduler.ExecutorInfo;

import java.util.Collection;
import java.util.Iterator;


@SuppressWarnings("unused")
public class ExecutorMessage {

    public ExecutorMessage(Collection<ExecutorData> executors) {
        this.executors = executors;
    }

    public String getType() {
        return type;
    }

    public Iterator<ExecutorData> getExecutors() {
        return executors.iterator();
    }

    public static String type = "EXECUTORS";

    public Collection<ExecutorData> executors;

    public static class ExecutorData {
        private String slaveId;
        private String executorId;

        public ExecutorData(String slaveId, String executorId) {
            this.slaveId = slaveId;
            this.executorId = executorId;
        }

        public static ExecutorData fromExecutor(ExecutorInfo executor) {
            return new ExecutorData(
                    executor.getSlaveID().getValue(),
                    executor.getExecutorID().getValue()
            );
        }

        public String getSlaveId() {
            return slaveId;
        }

        public String getExecutorId() {
            return executorId;
        }
    }
}
