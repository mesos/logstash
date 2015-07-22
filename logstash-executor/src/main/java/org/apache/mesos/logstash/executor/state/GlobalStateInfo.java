package org.apache.mesos.logstash.executor.state;

import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogSteamManager;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GlobalStateInfo {
    private final DockerClient dockerClient;
    private final DockerLogSteamManager streamManager;

    public GlobalStateInfo(DockerClient dockerClient, DockerLogSteamManager streamManager) {
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
            .addAllContainers(
                getRunningContainers().stream().map(c -> LogstashProtos.ContainerState.newBuilder()
                       .setType(getContainerStatus(c))
                       .setName(c)
                       .build()
                ).collect(Collectors.toList()))
            .build();
    }
}
