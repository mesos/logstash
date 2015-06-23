package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.command.EventCallback;
import com.github.dockerjava.api.model.Event;

import java.util.*;

/**
 * Created by ero on 22/06/15.
 */
public class DockerPoll {

    private DockerInfo dockerInfo = null;
    private Map<String, LogstashInfo> runningContainers = new HashMap<>();
    private List<FrameworkListener> frameworkListeners = new ArrayList<>();

    public DockerPoll(DockerInfo dockerInfo) {
        this.dockerInfo = dockerInfo;
        this.runningContainers = dockerInfo.getContainersThatWantsLogging();
        attachToDockerEventStream();
    }

    public void attach(FrameworkListener observer) {
        frameworkListeners.add(observer);
        notifyForEachNewContainer(observer, this.runningContainers);
    }

    private void attachToDockerEventStream() {
        dockerInfo.attachEventListener(new EventCallback() {
            @Override
            public void onEvent(Event event) {
                if (event.getStatus() == "stop") {
                    removeContainer(event.getId());
                }
                if (event.getStatus() == "start" || event.getStatus() == "restart") {
                    addContainer(event.getId());
                }
            }

            @Override
            public void onException(Throwable throwable) {

            }

            @Override
            public void onCompletion(int numEvents) {

            }

            @Override
            public boolean isReceiving() {
                return false;
            }
        });

    }

    private void addContainer(String containerId) {
        Map<String, LogstashInfo> runningContainers = dockerInfo.getContainersThatWantsLogging();
        if(runningContainers.containsKey(containerId)) {
            updateContainerState(runningContainers);
        }
    }

    private void removeContainer(String containerId) {
        if(runningContainers.containsKey(containerId)) {
            updateContainerState(dockerInfo.getContainersThatWantsLogging());
        }
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
