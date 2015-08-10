package org.apache.mesos.logstash.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.cluster.ClusterMonitor.ExecutionPhase;
import org.apache.mesos.logstash.state.LSTaskStatus;
import org.apache.mesos.logstash.state.StateUtil;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains all reconcilation informations. Monitors updated tasks to determine when we're finished.
 */
public class ReconciliationMonitor {
    private static final org.slf4j.Logger LOGGER = LoggerFactory
        .getLogger(ReconciliationMonitor.class);

    private final Set<Protos.TaskID> tasks = new HashSet<>();

    public ReconciliationMonitor(List<Protos.TaskID> taskIDs) {
        tasks.addAll(taskIDs);
    }


    public void updateTask(Protos.TaskStatus status) {
        try {
            // Update cluster state, if necessary
            tasks.remove(status.getTaskId());
        } catch (IllegalStateException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }
    }

    public boolean hasReconciledAllTasks() {
        return tasks.isEmpty();
    }

    public void logRemainingTasks() {
        LOGGER.info("Reconciliation phase still ongoing:");
        for (Protos.TaskID taskID : tasks) {
            LOGGER.info(
                "  Waiting for status update for task-id: {}",
                taskID);
        }
    }
}
