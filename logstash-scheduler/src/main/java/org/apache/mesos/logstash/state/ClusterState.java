package org.apache.mesos.logstash.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.mesos.Protos.TaskID;

/**
 * Model of cluster state. User is able to add, remove and monitor task status.
 */
@Component
public class ClusterState {
    private static final Logger LOGGER = Logger.getLogger(ClusterState.class);
    private static final String STATE_LIST = "stateList";
    @Inject
    SerializableState state;
    @Inject
    FrameworkState frameworkState;

    /**
     * Get a list of all tasks with state
     * @return a list of TaskInfo
     */
    public List<TaskInfo> getTaskList() {
        List<TaskInfo> taskInfoList = null;
        try {
            taskInfoList = state.get(getKey());
        } catch (IOException ex) {
            LOGGER.info("Unable to get key for cluster state due to invalid frameworkID.");
        }
        return taskInfoList == null ? new ArrayList<>(0) : taskInfoList;
    }

    /**
     * Get the status of a specific task
     * @param taskID the taskID to retrieve the task status for
     * @return a POJO representing TaskInfo, TaskStatus and FrameworkID packets
     * @throws InvalidParameterException when the taskId does not exist in the Task list.
     */
    public LSTaskStatus getStatus(TaskID taskID) throws InvalidParameterException {
        TaskInfo taskInfo = getTask(taskID);
        return new LSTaskStatus(state, frameworkState.getFrameworkID(), taskInfo);
    }

    public void addTask(TaskInfo taskInfo) {
        LOGGER.debug("Adding TaskInfo to cluster for task: " + taskInfo.getTaskId().getValue());
        if (exists(taskInfo.getTaskId())) {
            removeTaskById(taskInfo.getTaskId());
        }
        List<TaskInfo> taskList = getTaskList();
        taskList.add(taskInfo);
        setTaskInfoList(taskList);
    }

    public void removeTaskBySlaveId(Protos.SlaveID slaveId) {
        setTaskInfoList(getTaskList().stream().filter(info -> !info.getSlaveId().getValue().equals(slaveId.getValue())).collect(Collectors.toList()));
    }

    public void removeTaskByExecutorId(Protos.ExecutorID executorId) {
        setTaskInfoList(getTaskList().stream().filter(info -> !info.getExecutor().getExecutorId().getValue().equals(executorId.getValue())).collect(Collectors.toList()));
    }

    public void removeTaskById(TaskID taskId) throws InvalidParameterException {
        setTaskInfoList(getTaskList().stream().filter(info -> !info.getTaskId().getValue().equals(taskId.getValue())).collect(Collectors.toList()));
    }

    public Boolean exists(TaskID taskId) {
        try {
            getStatus(taskId);
        } catch (InvalidParameterException e) {
            return false;
        }
        return true;
    }

    /**
     * Get the TaskInfo packet for a specific task.
     * @param taskID the taskID to retrieve the TaskInfo for
     * @return a TaskInfo packet
     * @throws InvalidParameterException when the taskId does not exist in the Task list.
     */
    private TaskInfo getTask(TaskID taskID) throws InvalidParameterException {
        LOGGER.debug("Getting taskInfo from cluster for task: " + taskID.getValue());
        List<TaskInfo> taskInfoList = getTaskList();
        TaskInfo taskInfo = null;
        for (TaskInfo info : taskInfoList) {
            if (info.getTaskId().equals(taskID)) {
                taskInfo = info;
                break;
            }
        }
        if (taskInfo == null) {
            throw new InvalidParameterException("Could not find executor with that task ID: " + taskID.getValue());
        }
        return taskInfo;
    }

    private String logTaskList(List<TaskInfo> taskInfoList) {
        return Arrays.toString(taskInfoList.stream().map(t -> t.getTaskId().getValue()).collect(Collectors.toList()).toArray());
    }

    private void setTaskInfoList(List<TaskInfo> taskInfoList) {
        LOGGER.debug("Writing executor state list: " + logTaskList(taskInfoList));
        try {
            SerializableZookeeperState.mkdirAndSet(getKey(), taskInfoList, state);
        } catch (IOException ex) {
            LOGGER.error("Could not write list of executor states to zookeeper: ", ex);
        }
    }

    private String getKey() {
        return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
    }

}
