package org.apache.mesos.logstash.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.FrameworkConfig;
import org.apache.mesos.logstash.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains all cluster information. Monitors state of cluster elements and is responsible for
 * task reconciliation.
 */
@Component
public class ClusterMonitor implements Observer, InitializingBean {
    private static final Logger LOGGER =  LoggerFactory.getLogger(ClusterMonitor.class);

    @Inject
    private ClusterState clusterState;

    @Inject
    FrameworkState frameworkState;

    @Inject
    LiveState liveState;

    ReconcileSchedule reconcileSchedule = new ReconcileSchedule();
    private ExecutionPhase executionPhase = ExecutionPhase.RECONCILING_TASKS; // default

    @Inject
    SerializableState serializableState;

    @Inject
    StatePath statePath;

    @Inject
    FrameworkConfig frameworkConfig;

    private ReconciliationMonitor reconciliationMonitor = new ReconciliationMonitor(new ArrayList<>());

    @Override
    public void afterPropertiesSet() throws Exception {
        clusterState.getTaskList().forEach(this::monitorTask); // Get all previous executors and start monitoring them.
    }

    public synchronized ExecutionPhase getExecutionPhase() {
        return executionPhase;
    }

    /**
     * Monitor a new, or existing task.
     * @param task The task to monitor
     */
    public void monitorTask(TaskInfo task) {
        addNewTaskToCluster(task);
    }

    private LSTaskStatus addNewTaskToCluster(TaskInfo taskInfo) {
        LSTaskStatus taskStatus = new LSTaskStatus(serializableState, frameworkState.getFrameworkID(), taskInfo, statePath);
        taskStatus.setStatus(taskStatus.getDefaultStatus()); // This is a new task, so set default state until we get an update
        clusterState.addTask(taskInfo);
        return taskStatus;
    }

    public synchronized List<TaskInfo> getRunningTasks(){
        return clusterState.getTaskList().stream()
                .map(taskInfo -> clusterState.getStatus(taskInfo.getTaskId()))
                .filter(LSTaskStatus::isRunning)
                .map(LSTaskStatus::getTaskInfo)
                .collect(Collectors.toList());
    }


    private void updateTask(TaskStatus status) {
        try {
            // Update cluster state, if necessary
            if (clusterState.exists(status.getTaskId())) {


                LOGGER.debug("Updating task status for {} ", status);
                LSTaskStatus executorState = clusterState.getStatus(status.getTaskId());
                // Update state of Executor
                executorState.setStatus(status);

                if (executorState.taskInTerminalState()) {
                    LOGGER.error("Task in terminal state. Removing from zookeeper: {} ", executorState);
                    clusterState.removeTask(executorState.getTaskInfo()); // Remove task from cluster state.
                    executorState.destroy(); // Destroy task in ZK.
                }

                liveState.updateTaskStatus(status, executorState.getTaskInfo());

                if (isReconciling()) {
                    handleReconciliation(status);
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
            this.updateTask((TaskStatus) arg);
        } catch (ClassCastException e) {
            LOGGER.warn("Received update message, but it was not of type TaskStatus", e);
        }
    }

    public void startReconciling(SchedulerDriver driver) {
        List<Protos.TaskID> taskIds = clusterState.getTaskIdList();
        if (!taskIds.isEmpty()) {
            executionPhase = ExecutionPhase.RECONCILING_TASKS;
            reconciliationMonitor = new ReconciliationMonitor(taskIds);
            liveState.reset();

            new ReconcileStateTask(driver).start();
        } else {
            stopReconciling();
        }
    }

    public void stopReconciling() {
        executionPhase = ExecutionPhase.RECONCILIATION_DONE;
        reconciliationMonitor.reset();

        LOGGER.info("Finishing reconciliation phase");
    }

    public boolean isReconciling() {
        return executionPhase == ExecutionPhase.RECONCILING_TASKS;
    }

    private void handleReconciliation(TaskStatus status) {
        reconciliationMonitor.updateTask(status);

        if(reconciliationMonitor.hasReconciledAllTasks()) {
            stopReconciling();
        }
        else {
            reconciliationMonitor.logRemainingTasks();
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
        RECONCILIATION_DONE
    }

    static class ReconcileSchedule {
        public void schedule(TimerTask task, long startInSek){
            Timer timer = new Timer();
            timer.schedule(task, startInSek);
        }
    }

    class ReconcileStateTask extends TimerTask {

        static final int MAX_RETRY = 10; // TODO make configurable?
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

        public long getTimeout() {
            return frameworkConfig.getReconcilationTimeoutMillis() * (1 + count);
        }

        public void start() {
            if (executionPhase == ExecutionPhase.RECONCILING_TASKS) {
                driver.reconcileTasks(getRemainingTasksToReconcile());
                reconcileSchedule.schedule(this, getTimeout());
            }
        }

        @Override
        public void run() {
            if (!isReconciling()) {
                return;
            }
            LOGGER.info(
                    "Reconciliation phase has timed out. Waiting for tasks. Trigger new...");

            if (count < MAX_RETRY){

                new ReconcileStateTask(driver, count + 1).start(); // schedule again with increased timeout

            } else  {
                LOGGER.info(
                    "Max retries to reconcile tasks exceeded. Removing all tasks to which we haven't received a status update.");

                List<TaskInfo> taskInfos = new ArrayList<>(clusterState.getTaskList());
                Set<String> runningTaskIds = liveState.getNonTerminalTasks().stream().map(
                        t -> t.getTaskId().getValue()).collect(Collectors.toSet());

                taskInfos.stream()
                        .filter(task -> task != null && !runningTaskIds.contains(task.getTaskId().getValue()))
                        .forEach(task -> {
                            LOGGER.info("Removing task id: {}", task);
                            clusterState.removeTask(task);
                        });

                stopReconciling();
            }
        }

        private List<TaskStatus> getRemainingTasksToReconcile(){
            return reconciliationMonitor.getRemainingTaskIdsToReconcile().stream().map(t -> clusterState.getStatus(t).getStatus()).collect(Collectors.toList());
        }

    }
}
