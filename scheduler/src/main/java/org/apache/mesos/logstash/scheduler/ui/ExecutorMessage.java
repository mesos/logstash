package org.apache.mesos.logstash.scheduler.ui;

import java.util.Iterator;
import java.util.List;

@SuppressWarnings("unused")
public class ExecutorMessage {

    public ExecutorMessage(List<ExecutorInfo> executors) {
        this.executors = executors;
    }

    public String getType() {
        return type;
    }

    public Iterator<ExecutorInfo> getExecutors() {
        return executors.iterator();
    }

    public static String type = "EXECUTORS";

    public List<ExecutorInfo> executors;

    public static class ExecutorInfo {
        private String slaveId;
        private String executorId;

        public ExecutorInfo(String slaveId, String executorId) {
            this.slaveId = slaveId;
            this.executorId = executorId;
        }

        public static ExecutorInfo fromExecutor(Executor executor) {
            return new ExecutorInfo(
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
