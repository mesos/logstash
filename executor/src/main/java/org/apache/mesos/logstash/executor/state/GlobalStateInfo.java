package org.apache.mesos.logstash.executor.state;

import org.apache.mesos.logstash.common.LogstashProtos;
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
    private final DockerInfoCache dockerInfoCache;

    public GlobalStateInfo(DockerClient dockerClient, DockerLogSteamManager streamManager,
        DockerInfoCache dockerInfoCache) {
        this.dockerClient = dockerClient;
        this.streamManager = streamManager;
        this.dockerInfoCache = dockerInfoCache;
    }

    Set<String> getRunningContainers() {
        return dockerClient.getRunningContainers();
    }

    Set<String> getProcessedContainers() {
        return streamManager.getProcessedContainers();
    }

    List<String> getDockerFrameworkNamesWhichHaveBeenConfigured() {
        return dockerInfoCache.dockerInfos.stream().map(FrameworkInfo::getName)
            .collect(Collectors.toList());
    }

    public ExecutorMessage getStateAsExecutorMessage() {
        return ExecutorMessage.newBuilder().setType("GlobalStateInfo")
            .setGlobalStateInfo(LogstashProtos.GlobalStateInfo.newBuilder()
                .addAllConfiguredDockerFramework(getDockerFrameworkNamesWhichHaveBeenConfigured())
                .addAllProcessedContainer(getProcessedContainers())
                .addAllRunningContainer(getRunningContainers())
                .build())
            .build();
    }
}
