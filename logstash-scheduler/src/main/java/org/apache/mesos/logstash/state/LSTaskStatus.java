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
    public static final String STATE_KEY = "state";
    public static final String DEFAULT_STATUS_NO_MESSAGE_SET = "Default status. No message set.";
    private static final String FINGERPRINT_KEY = "configuration";
    private final SerializableState state;
    private final FrameworkID frameworkID;

    private final TaskInfo taskInfo;

    private final StatePath statePath;
    public LSTaskStatus(SerializableState state, FrameworkID frameworkID, TaskInfo taskInfo) {
        if (state == null) {
            throw new InvalidParameterException("State cannot be null");
        } else if (frameworkID == null || frameworkID.getValue().isEmpty()) {
            throw new InvalidParameterException("FrameworkID cannot be null or empty");
        }
        this.state = state;
        this.frameworkID = frameworkID;
        this.taskInfo = taskInfo;
        statePath = new StatePath(state);
    }

    public void setConfigurationFingerprint(String fingerprint) {
        try {
            LOGGER.debug("Writing configuration fingerprint to zk: [" + fingerprint + "] " + taskInfo.getTaskId().getValue());
            statePath.mkdir(getKey(FINGERPRINT_KEY));
            state.set(getKey(FINGERPRINT_KEY), fingerprint);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write configuration fingerprint to zookeeper", e);
        }
    }

    public String getConfigurationFingerprint() throws IllegalStateException {
        try {
            return state.get(getKey(FINGERPRINT_KEY));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get task status from zookeeper", e);
        }
    }

    public void setStatus(TaskStatus status) throws IllegalStateException {
        try {
            LOGGER.debug("Writing task status to zk: [" + status.getState() + "] " + status.getTaskId().getValue());
            statePath.mkdir(getKey(STATE_KEY));
            state.set(getKey(STATE_KEY), status);
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

    public TaskInfo getTaskInfo() {
        return taskInfo;
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

    public boolean isRunning(){
        TaskState state = getStatus().getState();
        return  TaskState.TASK_RUNNING.equals(state);
    }


    public boolean taskInTerminalState() {
        TaskState state = getStatus().getState();
        return StateUtil.isTerminalState(state);
    }

    public void destroy() {
        safeDeleteKey(STATE_KEY);
        safeDeleteKey(FINGERPRINT_KEY);
    }

    private void safeDeleteKey(String key) {
        try {
            state.delete(getKey(key));
        } catch (IOException e) {
            LOGGER.error("Could not destroy Task in ZK.", e);
        }
    }
}
