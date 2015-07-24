package org.apache.mesos.logstash.executor.state;

import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.executor.LogstashService;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogStreamManager;

import java.util.Set;
import java.util.stream.Collectors;

public class LiveState {
    private final LogstashService logstash;
    private final DockerClient dockerClient;
    private final DockerLogStreamManager streamManager;

    public LiveState(LogstashService logstash, DockerClient dockerClient,
        DockerLogStreamManager streamManager) {
        this.logstash = logstash;
        this.dockerClient = dockerClient;
        this.streamManager = streamManager;
    }

    Set<String> getRunningContainers() {
        return dockerClient.getRunningContainers();
    }

    Set<String> getProcessedContainers() {
        return streamManager.getProcessedContainers();
    }

    private LoggingStateType getContainerStatus(String containerId) {
        if (getProcessedContainers().contains(containerId)) {
            return LoggingStateType.STREAMING;
        }

        return LoggingStateType.NOT_STREAMING;
    }

    public ExecutorMessage getStateAsExecutorMessage() {
        return ExecutorMessage.newBuilder()
            .setType(ExecutorMessage.ExecutorMessageType.STATS)
            .setStatus(logstash.status())
            .addAllContainers(
                getRunningContainers().stream().map(c -> LogstashProtos.ContainerState.newBuilder()
                        .setType(getContainerStatus(c))
                        .setContainerId(c)
                        .setImageName("Container") // TODO: Get the image name here.
                        .build()
                ).collect(Collectors.toList()))
            .build();
    }
}
