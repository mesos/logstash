package org.apache.mesos.logstash.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.LSTaskStatus;
import org.apache.mesos.logstash.state.LiveState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

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

                if (isReconciling()) {
                    reconciliationMonitor.updateTask(status);

                    if(reconciliationMonitor.hasReconciledAllTasks()) {
                        stopReconciling();
                    }
                    else {
                        reconciliationMonitor.logRemainingTasks();
                    }
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

    public void startReconciling(SchedulerDriver driver) {
        List<Protos.TaskID> taskIds = clusterState.getTaskIdList();
        if (!taskIds.isEmpty()) {
            executionPhase = ExecutionPhase.RECONCILING_TASKS;
            this.reconciliationMonitor = new ReconciliationMonitor(clusterState.getTaskIdList());

            new ReconcileStateTask(driver).start();
        } else {
            // There is nothing to reconcile so just go ahead and mark the reconciliation as done.
            executionPhase = ExecutionPhase.RECONCILIATION_DONE;
        }
    }

    public void stopReconciling() {
        executionPhase = ExecutionPhase.RECONCILIATION_DONE;
        reconciliationMonitor = null;

        LOGGER.info("Finishing reconciliation phase");
    }

    public boolean isReconciling() {
        return reconciliationMonitor != null;
    }

    public enum ExecutionPhase {

        /**
         * Waits here for the timeout on (re)registration.
         */
        RECONCILING_TASKS,

        /**
         * Everything is reconciled
         */
        RECONCILIATION_DONE
    }

    private class ReconcileStateTask extends TimerTask {

        final int maxRetry = 10; // TODO make configurable?
        final int count;
        private SchedulerDriver driver;

        private ReconcileStateTask(SchedulerDriver driver) {
            this.count = 0;
            this.driver = driver;
        }

        private ReconcileStateTask(SchedulerDriver driver, int count) {
            this.count = count;
            this.driver = driver;
        }

        public int getTimeout() {
            return configuration.getReconcilationTimeoutMillis() * (1 + count);
        }

        public void start() {
            List<Protos.TaskID> taskIds = clusterState.getTaskIdList();
            if (taskIds.isEmpty()) {
                stopReconciling();
            }
            else {
                executionPhase = ExecutionPhase.RECONCILING_TASKS;
                reconciliationMonitor = new ReconciliationMonitor(taskIds);


                liveState.reset();
                driver.reconcileTasks(getRemainingTasksToReconcile());

                Timer timer = new Timer();
                timer.schedule(this, getTimeout());

            }
        }

        @Override
        public void run() {
            if (!isReconciling()) {
                return;
            }
            LOGGER.info(
                    "Reconciliation phase has timed out. Waiting for {} tasks. Trigger new...");

            if (count < maxRetry){

                new ReconcileStateTask(driver, count + 1).start();

            } else  {
                LOGGER.info("Max retries to reconcile tasks exceeded. Removing all tasks to which we haven't received a status update.");

                List<Protos.TaskInfo> taskInfos = getClusterState().getTaskList();
                Set<String> runningTaskIds = liveState.getNonTerminalTasks().stream().map(
                        t -> t.getTaskId().getValue()).collect(Collectors.toSet());

                taskInfos.stream()
                        .filter(task -> task != null && !runningTaskIds.contains(task.getTaskId().getValue()))
                        .forEach(task -> {
                            LOGGER.info("Removing task id: {}", task);
                            getClusterState().removeTask(task);
                        });

                stopReconciling();
            }
        }

        private List<Protos.TaskStatus> getRemainingTasksToReconcile(){
            return clusterState.getTaskIdList().stream().map(t -> clusterState.getStatus(t).getStatus()).collect(Collectors.toList());
        }

    }
}
