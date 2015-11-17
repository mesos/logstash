package org.apache.mesos.logstash.executor.state;

import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.executor.LogstashService;
import org.apache.mesos.logstash.executor.docker.DockerClient;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * State for the executor.
 */
public class LiveState {
    private final LogstashService logstash;
    private final DockerClient dockerClient;
    private String hostName = "unknown hostname";

    public LiveState(LogstashService logstash, DockerClient dockerClient) {
        this.logstash = logstash;
        this.dockerClient = dockerClient;
    }

    Set<String> getRunningContainers() {
        return dockerClient.getRunningContainers();
    }

    private LoggingStateType getContainerStatus(String containerId) {
        return LoggingStateType.NOT_STREAMING;
    }

    public ExecutorMessage getStateAsExecutorMessage() {
        return ExecutorMessage.newBuilder()
            .setType(ExecutorMessage.ExecutorMessageType.STATS)
            .setStatus(logstash.status())
            .setHostName(hostName)
            .addAllContainers(
                    getRunningContainers().stream().map(c -> LogstashProtos.ContainerState.newBuilder()
                                    .setType(getContainerStatus(c))
                                    .setContainerId(c)
                                    .setImageName(dockerClient.getImageNameOfContainer(c))
                                    .build()
                    ).collect(Collectors.toList()))
            .build();
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
}
