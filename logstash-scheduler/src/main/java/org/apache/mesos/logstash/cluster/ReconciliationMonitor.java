package org.apache.mesos.logstash.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.cluster.ClusterMonitor.ExecutionPhase;
import org.apache.mesos.logstash.state.LSTaskStatus;
import org.apache.mesos.logstash.state.StateUtil;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

/**
 * Contains all reconcilation informations. Monitors updated tasks to determine when we're finished.
 */
public class ReconciliationMonitor implements Observer {
    private static final org.slf4j.Logger LOGGER = LoggerFactory
        .getLogger(ReconciliationMonitor.class);
    private final ClusterMonitor clusterMonitor;

    private final Map<Protos.TaskID, LSTaskStatus> tasks = new HashMap<>();

    public ReconciliationMonitor(ClusterMonitor clusterMonitor) {
        if (clusterMonitor == null) {
            throw new InvalidParameterException("Constructor parameters cannot be null.");
        }
        this.clusterMonitor = clusterMonitor;

        this.clusterMonitor.getClusterState().getTaskList()
            .forEach(this::monitorTask); // Get all previous executors and start monitoring them.

        correctExecutionPhase();
    }

    /**
     * Monitor all non-terminal tasks we have to wait for status updates
     * @param task The task to monitor
     */
    public void monitorTask(Protos.TaskInfo task) {
        LSTaskStatus status = clusterMonitor.getClusterState().getStatus(task.getTaskId());

        if (status != null && !StateUtil.isTerminalState(status.getStatus().getState())) {
            tasks.put(task.getTaskId(), status);
        }
    }

    public void updateTask(Protos.TaskStatus status) {
        try {
            // Update cluster state, if necessary
            if (clusterMonitor.getClusterState().exists(status.getTaskId())) {

                if (tasks.containsKey(status.getTaskId())) {
                    tasks.remove(status.getTaskId());
                }
            } else {
                LOGGER.warn("Could not find task in cluster state.");
            }
        } catch (IllegalStateException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }

        correctExecutionPhase();

    }

    @Override
    public void update(Observable o, Object arg) {
        try {
            this.updateTask((Protos.TaskStatus) arg);
        } catch (ClassCastException e) {
            LOGGER.warn("Received update message, but it was not of type TaskStatus", e);
        }
    }

    private void correctExecutionPhase() {
        if (tasks.size() == 0) {
            LOGGER.info("Finished reconciliation phase.");
            clusterMonitor.setExecutionPhase(ExecutionPhase.RUNNING); // nothing to reconcile
        } else {
            LOGGER.info("Reconciliation phase still ongoing:");
            for (Protos.TaskID taskID : tasks.keySet()) {
                LOGGER.info(
                    "  Waiting for status update for task-id: {}",
                    taskID);
            }
        }
    }
}
