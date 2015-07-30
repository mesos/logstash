package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogStreamManager;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Class responsible for connecting each discovered framework to logstash
 */
public class ConfigManager {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private final LogstashService logstash;
    private ContainerizerClient containerizerClient;
    private DockerLogStreamManager dockerLogStreamManager;

    public List<LogstashConfig> dockerInfo = new ArrayList<>();
    private List<LogstashConfig> hostInfo = new ArrayList<>();

    public ConfigManager(LogstashService logstash, ContainerizerClient containerizerClient,
        DockerLogStreamManager dockerLogStreamManager) {
        this.logstash = logstash;
        this.containerizerClient = containerizerClient;
        this.dockerLogStreamManager = dockerLogStreamManager;
        this.containerizerClient.setDelegate(this::onContainerListUpdated);
    }

    public void onNewConfigsFromScheduler(List<LogstashConfig> hostInfo,
        List<LogstashConfig> dockerInfo) {
        LOGGER.info("onNewConfigsFromScheduler, {}\n-------\n{}", dockerInfo, hostInfo);
        this.dockerInfo = dockerInfo;
        this.hostInfo = hostInfo;
        LOGGER.info("New configuration received. Reconfiguring...");
        updateDockerStreams();
        logstash.update(dockerInfo, hostInfo);
    }

    private void onContainerListUpdated(List<String> images) {
        LOGGER.info("New Containers Discovered. Reconfiguring...");
        updateDockerStreams();
        logstash.update(dockerInfo, hostInfo);
    }

    private synchronized void updateDockerStreams() {

        // On new configs received or new containers

        // - Find running containers that have a matching config.

        Function<String, LogstashConfig> lookupConfig = createLookupHelper(dockerInfo);

        Predicate<String> hasKnownConfig = c -> lookupConfig.apply(c) != null;
        Predicate<String> hasUnknownConfig = c -> lookupConfig.apply(c) == null;

        Function<String, DockerFramework> createFramework = c -> new DockerFramework(lookupConfig.apply(c), new DockerFramework.ContainerId(c));

        Stream<DockerFramework> frameworks = containerizerClient
            .getRunningContainers()
            .stream()
            .filter(hasKnownConfig)
            .map(createFramework);

        // - For each new running container start streaming logs.

        frameworks.forEach(dockerLogStreamManager::setupContainerLogfileStreaming);

        Set<String> frameworksToStopStreaming = dockerLogStreamManager.getProcessedContainers()
            .stream().filter(hasUnknownConfig).collect(Collectors.toSet());

        frameworksToStopStreaming.stream()
            .forEach(dockerLogStreamManager::stopStreamingForWholeFramework);
    }

    private Function<String, LogstashConfig> createLookupHelper(
        List<LogstashConfig> logstashInfos) {

        Map<String, LogstashConfig> logstashInfoMap = logstashInfos
            .stream()
            .collect(toMap(LogstashConfig::getFrameworkName, x -> x));

        return c -> logstashInfoMap.get(containerizerClient.getImageNameOfContainer(c));
    }
}
