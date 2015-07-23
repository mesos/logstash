package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogSteamManager;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Class responsible for connecting each discovered framework to logstash
 */
public class ConfigManager {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private final LogstashService logstash;
    private ContainerizerClient containerizerClient;
    private DockerLogSteamManager dockerLogSteamManager;

    public List<FrameworkInfo> dockerInfo = new ArrayList<>();
    private List<FrameworkInfo> hostInfo = new ArrayList<>();

    public ConfigManager(LogstashService logstash, ContainerizerClient containerizerClient,
        DockerLogSteamManager dockerLogSteamManager) {
        this.logstash = logstash;
        this.containerizerClient = containerizerClient;
        this.dockerLogSteamManager = dockerLogSteamManager;
        this.containerizerClient.setDelegate(this::onContainerListUpdated);
    }

    public void onNewConfigsFromScheduler(List<FrameworkInfo> hostInfo, List<FrameworkInfo> dockerConfig) {
        this.dockerInfo = dockerConfig;
        this.hostInfo = hostInfo;
        updateDockerStreams();
        logstash.update(dockerInfo, hostInfo);
    }

    private void onContainerListUpdated(List<String> images) {
        LOGGER.info("New Containers Discovered. Reconfiguring.");
        updateDockerStreams();
        logstash.update(dockerInfo, hostInfo);
    }

    private synchronized void updateDockerStreams() {

        // On new configs received or new containers

        // - Find running containers that have a matching config.

        Function<String, FrameworkInfo> lookupConfig = createLookupHelper(dockerInfo);

        Predicate<String> hasKnownConfig = c -> lookupConfig.apply(c) != null;
        Function<String, DockerFramework> createFramework = c -> new DockerFramework(
            lookupConfig.apply(c), new DockerFramework.ContainerId(c));

        Stream<DockerFramework> frameworks = containerizerClient
            .getRunningContainers()
            .stream()
            .filter(hasKnownConfig)
            .map(createFramework);

        // - For each new running container start streaming logs.

        frameworks.forEach(dockerLogSteamManager::setupContainerLogfileStreaming);
    }

    private Function<String, FrameworkInfo> createLookupHelper(List<FrameworkInfo> logstashInfos) {

        Map<String, FrameworkInfo> logstashInfoMap = logstashInfos
            .stream()
            .collect(toMap(FrameworkInfo::getName, x -> x));

        return c -> logstashInfoMap.get(containerizerClient.getImageNameOfContainer(c));
    }
}
