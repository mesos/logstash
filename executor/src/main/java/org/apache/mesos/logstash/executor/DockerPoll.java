package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.command.EventCallback;
import com.github.dockerjava.api.model.Event;
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
        this.dockerInfo = dockerInfo;
        this.runningContainers = dockerInfo.getContainersThatWantLogging();
        attachToDockerEventStream();
    }

    public void attach(FrameworkListener observer) {
        frameworkListeners.add(observer);
        // NOTE: DANGEROUS RACE CONDITION!!!
        notifyForEachNewContainer(observer, this.runningContainers);
    }

    private void attachToDockerEventStream() {
        dockerInfo.attachEventListener(new EventCallback() {
            @Override
            public void onEvent(Event event) {
                LOGGER.info("Got event!!!!!! " + event.getStatus());
                if (event.getStatus().equalsIgnoreCase("stop") || event.getStatus().equalsIgnoreCase("die")) {
                    updateContainerState(dockerInfo.getContainersThatWantLogging());
                }
                if (event.getStatus().equalsIgnoreCase("start") || event.getStatus().equalsIgnoreCase("restart")) {
                    updateContainerState(dockerInfo.getContainersThatWantLogging());
                }
            }

            @Override
            public void onException(Throwable throwable) {
                LOGGER.error("exception :(" + throwable.toString());
            }

            @Override
            public void onCompletion(int numEvents) {
                LOGGER.info("Completed!!!!!! ");
            }

            @Override
            public boolean isReceiving() {
                return true;
            }
        });

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
            frameWorkListener.frameworkRemoved(new Framework(entry));
        }
    }

    private void notifyForEachNewContainer(FrameworkListener frameWorkListener, Map<String, LogstashInfo> newContainers) {
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
