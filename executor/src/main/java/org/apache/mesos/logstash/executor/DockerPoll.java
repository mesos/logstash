package org.apache.mesos.logstash.executor;

import javafx.util.Pair;

import java.util.*;

/**
 * Created by ero on 22/06/15.
 */
public class DockerPoll {

    private DockerInfo dockerInfo = null;
    private Map<String, LogstashInfo> runningContainers = null;
    private List<FrameworkListener> frameworkListeners = new ArrayList<>();

    public DockerPoll(DockerInfo dockerInfo) {
        this.dockerInfo = dockerInfo;
    }

    public void attach(FrameworkListener observer) {
        frameworkListeners.add(observer);
    }

    public void poll(long delay, long interval) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(createTimerTask(), delay, interval);
    }

    private TimerTask createTimerTask() {
        return new TimerTask() {
            @Override
            public void run() {
                Map<String, LogstashInfo> newContainerState = dockerInfo.getContainersThatWantsLogging();
                Map<String, LogstashInfo> removedContainers = diffingContainers(runningContainers, newContainerState);
                Map<String, LogstashInfo> addedContainers = diffingContainers(newContainerState, runningContainers);
                notifyNewContainerState(removedContainers, addedContainers);
                runningContainers = newContainerState;
            }
        };
    }

    private void notifyNewContainerState(Map<String, LogstashInfo> removedContainers, Map<String, LogstashInfo> addedContainers) {
        for (FrameworkListener frameWorkListener : frameworkListeners) {
            notifyForEachRemovedContainer(frameWorkListener, removedContainers);
            notifyForEachNewContainer(frameWorkListener, addedContainers);
        }
    }

    private void notifyForEachRemovedContainer(FrameworkListener frameWorkListener, Map<String, LogstashInfo> removedContainers) {
        for (Map.Entry<String, LogstashInfo> entry : removedContainers.entrySet()) {
            frameWorkListener.FrameworkRemoved(new Framework(entry));
        }
    }

    private void notifyForEachNewContainer(FrameworkListener frameWorkListener, Map<String, LogstashInfo> newContainers) {
        for (Map.Entry<String, LogstashInfo> entry : newContainers.entrySet()) {
            frameWorkListener.FrameworkAdded(new Framework(entry));
        }
    }


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
