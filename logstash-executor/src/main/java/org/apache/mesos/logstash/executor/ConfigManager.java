package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Class responsible for updating configurations and corresponding docker streams.
 */
public class ConfigManager {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private final LogstashService logstash;
    private DockerClient containerizerClient;

    public List<LogstashConfig> dockerInfo = new ArrayList<>();
    private List<LogstashConfig> hostInfo = new ArrayList<>();

    public ConfigManager(LogstashService logstash, DockerClient containerizerClient) {
        this.logstash = logstash;
        this.containerizerClient = containerizerClient;
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

        Map<String, LogstashConfig> logstashConfigsByFrameworkName = dockerInfo
            .stream()
            .collect(toMap(LogstashConfig::getFrameworkName, x -> x));

        Function<String, LogstashConfig> lookupConfig = c1 -> logstashConfigsByFrameworkName.get(containerizerClient.getImageNameOfContainer(c1));

        containerizerClient
            .getRunningContainers()
            .stream()
            .forEach(c -> { if (lookupConfig.apply(c) != null) { new DockerFramework(lookupConfig.apply(c)); } });

    }

}
