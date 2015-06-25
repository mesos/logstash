package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by ero on 22/06/15.
 */
public class DockerPoll {
    public static final Logger LOGGER = Logger.getLogger(DockerPoll.class.toString());

    private DockerInfo dockerInfo = null;
    private Map<String, LogstashInfo> runningContainers = new HashMap<>();
    private List<FrameworkListener> frameworkListeners = new ArrayList<>();

    public DockerPoll(DockerInfo dockerInfo) {
        this(dockerInfo, 5000);
    }

    public DockerPoll(DockerInfo dockerInfo, long pollInterval) {
        this.dockerInfo = dockerInfo;
        startPoll(pollInterval);
    }

    public void attach(FrameworkListener observer) {
        frameworkListeners.add(observer);
        notifyForEachNewContainer(observer, this.runningContainers);
    }

    private void startPoll(long pollInterval) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateContainerState(dockerInfo.getContainersThatWantLogging());
            }
        }, 0, pollInterval);
    }

    private void updateContainerState(Map<String, LogstashInfo> newContainerState) {
        notifyNewContainerState(newContainerState, runningContainers);
        runningContainers = new HashMap<>(newContainerState);
    }

    private void notifyNewContainerState(Map<String, LogstashInfo> newContainerState, Map<String, LogstashInfo> runningContainers) {
        Map<String, LogstashInfo> removedContainers = diffingContainers(runningContainers, newContainerState);
        Map<String, LogstashInfo> addedContainers = diffingContainers(newContainerState, runningContainers);
        for (FrameworkListener frameWorkListener : frameworkListeners) {
            notifyForEachRemovedContainer(frameWorkListener, removedContainers);
            notifyForEachNewContainer(frameWorkListener, addedContainers);
        }
    }

    private void notifyForEachRemovedContainer(FrameworkListener frameWorkListener, Map<String, LogstashInfo> removedContainers) {
        LOGGER.info(String.format("Notifying about %d removed containers", removedContainers.size()));
        for (Map.Entry<String, LogstashInfo> entry : removedContainers.entrySet()) {
            frameWorkListener.frameworkRemoved(new Framework(entry));
        }
    }

    private void notifyForEachNewContainer(FrameworkListener frameWorkListener, Map<String, LogstashInfo> newContainers) {
        LOGGER.info(String.format("Notifying about %d new containers", newContainers.size()));
        for (Map.Entry<String, LogstashInfo> entry : newContainers.entrySet()) {
            frameWorkListener.frameworkAdded(new Framework(entry));
        }
    }

    // compute the complement of subSet, relative to set
    private Map<String,LogstashInfo> diffingContainers(Map<String, LogstashInfo> set, Map<String, LogstashInfo> subSet) {
        Map<String, LogstashInfo> diff = new HashMap<>();
        for (Map.Entry<String, LogstashInfo> entry : set.entrySet()) {
            if(!subSet.containsKey(entry.getKey())) {
                diff.put(entry.getKey(), entry.getValue());
            }
        }
        return diff;
    }
}
