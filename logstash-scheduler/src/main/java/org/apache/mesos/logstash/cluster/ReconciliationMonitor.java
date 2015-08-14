package org.apache.mesos.logstash.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Contains all reconciliation information. Monitors updated tasks to determine when we're finished.
 */
public class ReconciliationMonitor {
    private static final org.slf4j.Logger LOGGER = LoggerFactory
        .getLogger(ReconciliationMonitor.class);

    private final Set<TaskID> tasks = new HashSet<>();

    public ReconciliationMonitor(List<TaskID> taskIDs) {
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

    public List<TaskID> getRemainingTaskIdsToReconcile(){
        return new ArrayList<>(tasks);
    }

    public void reset(){
        tasks.clear();
    }

    public boolean hasReconciledAllTasks() {
        return tasks.isEmpty();
    }

    public void logRemainingTasks() {
        LOGGER.info("Reconciliation phase still ongoing:");
        for (TaskID taskID : tasks) {
            LOGGER.info(
                "  Waiting for status update for task-id: {}",
                taskID);
        }
    }
}
