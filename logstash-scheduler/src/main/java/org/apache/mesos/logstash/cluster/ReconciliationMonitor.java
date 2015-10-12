package org.apache.mesos.logstash.cluster;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Contains all reconciliation information. Monitors updated tasks to determine when we're finished.
 */
public class ReconciliationMonitor {
    private static final Logger LOGGER = Logger.getLogger(ReconciliationMonitor.class.toString());

    private final Set<TaskID> tasks = new HashSet<>();

    public ReconciliationMonitor(List<TaskID> taskIDs) {
        tasks.addAll(taskIDs);
    }


    public void updateTask(Protos.TaskStatus status) {
        try {
            // Update cluster state, if necessary
            tasks.remove(status.getTaskId());
        } catch (IllegalStateException e) {
            LOGGER.error("Unable to write executor state to zookeeper");
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
            LOGGER.info(String.format(
                "  Waiting for status update for task-id: %s",
                taskID));
        }
    }
}
