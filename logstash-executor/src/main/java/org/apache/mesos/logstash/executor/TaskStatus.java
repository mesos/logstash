package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

/**
 * Wraps the TaskState status updates into easy to read methods.
 */
public class TaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class.getCanonicalName());
    private Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue("").build();
    private Protos.TaskStatus currentState = getTaskStatus(Protos.TaskState.TASK_STAGING);

    private Protos.TaskStatus getTaskStatus(Protos.TaskState taskState) {
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskID)
                .setState(taskState).build();
        currentState = status;
        return status;
    }

    public Protos.TaskStatus running() {
        LOGGER.info("TASK_RUNNING");
        return getTaskStatus(Protos.TaskState.TASK_RUNNING);
    }

    public Protos.TaskStatus failed() {
        LOGGER.info("TASK_FAILED");
        return getTaskStatus(Protos.TaskState.TASK_FAILED);
    }

    public Protos.TaskStatus error() {
        LOGGER.info("TASK_ERROR");
        return getTaskStatus(Protos.TaskState.TASK_ERROR);
    }

    public Protos.TaskStatus starting() {
        LOGGER.info("TASK_STARTING");
        return getTaskStatus(Protos.TaskState.TASK_STARTING);
    }

    public Protos.TaskStatus finished() {
        LOGGER.info("TASK_FINISHED");
        return getTaskStatus(Protos.TaskState.TASK_FINISHED);
    }

    public Protos.TaskStatus killed() {
        LOGGER.info(Protos.TaskState.TASK_KILLED.toString());
        return getTaskStatus(Protos.TaskState.TASK_KILLED);
    }

    public void setTaskID(Protos.TaskID taskID) {
        if (taskID == null) {
            throw new NullPointerException("TaskID cannot be null");
        }
        this.taskID = taskID;
    }

    public Protos.TaskStatus currentState() {
        return currentState;
    }
}