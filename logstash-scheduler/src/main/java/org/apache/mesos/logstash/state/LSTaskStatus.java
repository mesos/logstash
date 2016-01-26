package org.apache.mesos.logstash.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * Status of task. This is necessary because the raw TaskInfo packet doesn't contain the frameworkID or a link to
 * the respective TaskStatus packet.
 */
public class LSTaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class);
    private static final String STATE_KEY = "state";
    public static final String DEFAULT_STATUS_NO_MESSAGE_SET = "Default status. No message set.";
    private final SerializableState state;
    private final FrameworkID frameworkID;

    private final TaskInfo taskInfo;

    public LSTaskStatus(SerializableState state, FrameworkID frameworkID, TaskInfo taskInfo) {
        if (state == null) {
            throw new InvalidParameterException("State cannot be null");
        } else if (frameworkID == null || frameworkID.getValue().isEmpty()) {
            throw new InvalidParameterException("FrameworkID cannot be null or empty");
        }
        this.state = state;
        this.frameworkID = frameworkID;
        this.taskInfo = taskInfo;
    }

    public void setStatus(TaskStatus status) throws IllegalStateException {
        try {
            LOGGER.debug("Writing task status to zk: [" + status.getState() + "] " + status.getTaskId().getValue());
            SerializableZookeeperState.mkdirAndSet(getKey(STATE_KEY), status, state);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write task status to zookeeper", e);
        }
    }

    public TaskStatus getStatus() throws IllegalStateException {
        try {
            return state.get(getKey(STATE_KEY));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get task status from zookeeper", e);
        }
    }

    public TaskStatus getDefaultStatus() {
        return TaskStatus.newBuilder()
                    .setState(TaskState.TASK_STARTING)
                    .setTaskId(taskInfo.getTaskId())
                    .setExecutorId(taskInfo.getExecutor().getExecutorId())
                    .setMessage(DEFAULT_STATUS_NO_MESSAGE_SET)
                    .build();
    }

    @Override
    public String toString() {
        String retVal;
        try {
            retVal = getKey(STATE_KEY) + ": [" + getStatus().getState() + "] " +  getStatus().getMessage();
        } catch (Exception e) {
            retVal = getKey(STATE_KEY) + ": Unable to get message";
        }
        return retVal;
    }

    private String getKey(String path) {
        return frameworkID.getValue() + "/" + path + "/" + taskInfo.getTaskId().getValue();
    }
}
