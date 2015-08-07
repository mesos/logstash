package org.apache.mesos.logstash.state;
import org.apache.mesos.Protos;

public class StateUtil {
    public static boolean isTerminalState(Protos.TaskState taskStatus) {
        return taskStatus.equals(Protos.TaskState.TASK_FAILED)
            || taskStatus.equals(Protos.TaskState.TASK_FINISHED)
            || taskStatus.equals(Protos.TaskState.TASK_KILLED)
            || taskStatus.equals(Protos.TaskState.TASK_LOST)
            || taskStatus.equals(Protos.TaskState.TASK_ERROR);
    }

}
