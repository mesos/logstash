package org.apache.mesos.logstash.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.LSTaskStatus;
import org.apache.mesos.logstash.state.LiveState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

/**
 * Contains all cluster information. Monitors state of cluster elements.
 */
public class ClusterMonitor implements Observer {
    private static final Logger LOGGER =  LoggerFactory.getLogger(ClusterMonitor.class);
    private final Configuration configuration;
    private final ClusterState clusterState;
    private final LiveState liveState;
    private ExecutionPhase executionPhase = ExecutionPhase.RECONCILING_TASKS; // default

    private ReconciliationMonitor reconciliationMonitor = null;

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

    public synchronized ExecutionPhase getExecutionPhase() {
        return executionPhase;
    }


    public synchronized void setExecutionPhase(
        ExecutionPhase executionPhase) {

        if (ExecutionPhase.RUNNING == executionPhase){
            this.reconciliationMonitor = null;
            this.executionPhase = executionPhase;
        } else if (ExecutionPhase.RECONCILING_TASKS == executionPhase){
            createNewReconciliationMonitor(); // will set the execution phase on its own
        }
    }

    public void createNewReconciliationMonitor( ) {
        this.reconciliationMonitor = new ReconciliationMonitor(this);
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

    public synchronized List<Protos.TaskInfo> getRunningTasks(){
        ArrayList<Protos.TaskInfo> runningTasks = new ArrayList<>();
        List<Protos.TaskInfo> taskInfoList = getClusterState().getTaskList();
        for (Protos.TaskInfo taskInfo: taskInfoList){
            LSTaskStatus executorState = getClusterState().getStatus(taskInfo.getTaskId());
            if (executorState.isRunning()){
                runningTasks.add(executorState.getTaskInfo());
            }
        }
        return runningTasks;
    }


    public void updateTask(Protos.TaskStatus status) {
        try {
            // Update cluster state, if necessary
            if (getClusterState().exists(status.getTaskId())) {


                LOGGER.debug("Updating task status for {} ", status);
                LSTaskStatus executorState = getClusterState().getStatus(status.getTaskId());
                // Update state of Executor
                executorState.setStatus(status);
                // If task in error
                if (executorState.taskInTerminalState()) {
                    LOGGER.error("Task in terminal state. Removing from zookeeper: {} ", executorState);
                    clusterState.removeTask(executorState.getTaskInfo()); // Remove task from cluster state.
                    executorState.destroy(); // Destroy task in ZK.
                }

                liveState.updateTaskStatus(status, executorState.getTaskInfo());

                if (this.reconciliationMonitor != null) {
                    reconciliationMonitor.updateTask(status);
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

    public enum ExecutionPhase {

        /**
         * Waits here for the timeout on (re)registration.
         */
        RECONCILING_TASKS,

        /**
         * Everything is reconciled
         */
        RUNNING
    }
}
