package org.apache.mesos.logstash.cluster;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.LSTaskStatus;
import org.apache.mesos.logstash.state.LiveState;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

/**
 * Contains all cluster information. Monitors state of cluster elements.
 */
public class ClusterMonitor implements Observer {
    private static final Logger LOGGER = Logger.getLogger(ClusterMonitor.class);
    private final Configuration configuration;
    private final ClusterState clusterState;
    private final LiveState liveState;

    public ClusterMonitor(Configuration configuration,
        ClusterState clusterState, LiveState liveState) {
        if (configuration == null || clusterState == null || liveState == null) {
            throw new InvalidParameterException("Constructor parameters cannot be null.");
        }
        this.configuration = configuration;
        this.clusterState = clusterState;
        this.liveState = liveState;
        clusterState.getTaskList().forEach(this::monitorTask); // Get all previous executors and start monitoring them.
    }

    /**
     * Monitor a new, or existing task.
     * @param task The task to monitor
     */
    public void monitorTask(Protos.TaskInfo task) {
        addNewTaskToCluster(task);
    }

    public ClusterState getClusterState() {
        return clusterState;
    }


    private LSTaskStatus addNewTaskToCluster(Protos.TaskInfo taskInfo) {
        LSTaskStatus taskStatus = new LSTaskStatus(configuration.getState(), configuration.getFrameworkId(), taskInfo);
        taskStatus.setStatus(taskStatus.getDefaultStatus()); // This is a new task, so set default state until we get an update
        clusterState.addTask(taskInfo);
        return taskStatus;
    }


    public synchronized List<Protos.TaskStatus> getRunningTasks(){
        ArrayList<Protos.TaskStatus> runningTasks = new ArrayList<>();
        List<Protos.TaskInfo> taskInfoList = getClusterState().getTaskList();
        for (Protos.TaskInfo taskInfo: taskInfoList){
            LSTaskStatus executorState = getClusterState().getStatus(taskInfo.getTaskId());
            if (executorState.isRunning()){
                runningTasks.add(executorState.getStatus());
            }
        }
        return runningTasks;
    }

    public void updateTask(Protos.TaskStatus status) {
        try {
            // Update cluster state, if necessary
            if (getClusterState().exists(status.getTaskId())) {

                liveState.updateTaskStatus(status);

                LOGGER.debug("Updating task status for: " + status.getTaskId());
                LSTaskStatus executorState = getClusterState().getStatus(status.getTaskId());
                // Update state of Executor
                executorState.setStatus(status);
                // If task in error
                if (executorState.taskInError()) {
                    LOGGER.error("Task in error state. Performing reconciliation: " + executorState
                        .toString());
                    clusterState.removeTask(executorState.getTaskInfo()); // Remove task from cluster state.
                    executorState.destroy(); // Destroy task in ZK.
                }
            } else {
                LOGGER.warn("Could not find task in cluster state.");
            }
        } catch (IllegalStateException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        try {
            this.updateTask((Protos.TaskStatus) arg);
        } catch (ClassCastException e) {
            LOGGER.warn("Received update message, but it was not of type TaskStatus", e);
        }
    }
}
