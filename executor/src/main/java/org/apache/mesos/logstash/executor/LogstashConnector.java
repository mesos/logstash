package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Class responsible for connecting each discovered framework to logstashService
 * Created by peldan on 22/06/15.
 */
public class LogstashConnector implements LogConfigurationListener {
    public static final Logger LOGGER = Logger.getLogger(LogstashConnector.class.toString());

    private LogstashService logstashService;

    private DockerInfo dockerInfo;

    private LogfileStreaming logfileStreaming;

    public LogstashConnector(DockerInfo dockerInfo, LogstashService logstashService, LogfileStreaming logfileStreaming) {
        this.dockerInfo = dockerInfo;
        this.logstashService = logstashService;
        this.logfileStreaming = logfileStreaming;
    }

    @Override
    public void updatedHostLogConfigurations(List<HostFramework> frameworks) {
        String concatenated = frameworks.stream()
                .map(Framework::getConfiguration)
                .collect(Collectors.joining("\n"));
        logstashService.setStaticConfig(concatenated);
    }

    @Override
    public void updatedDockerLogConfigurations(List<DockerFramework> frameworks) {

        LOGGER.info(String.format("Number of frameworks %d", frameworks.size()));

        Map<String, DockerFramework> containerConfiguration = getPerContainerConfiguration(frameworks);

        LOGGER.info(String.format("Number of containers to configure %d", containerConfiguration.size()));
        for (String containerId : containerConfiguration.keySet()) {

            DockerFramework framework = containerConfiguration.get(containerId);

            if (logfileStreaming.isConfigured(containerId)) {
                LOGGER.info(String.format("Skipping %s (%s) because it is already configured", containerId, framework.getName()));
                continue;
            }

            logfileStreaming.setupContainerLogfileStreaming(containerId, framework);
            logstashService.reconfigure(containerConfiguration);
            assertStarted();
        }
    }

    private void assertStarted() {
        if (!logstashService.hasStarted()) {
            logstashService.start();
        }
    }

    private Map<String, DockerFramework> getPerContainerConfiguration(List<DockerFramework> frameworks) {
        Map<String, DockerFramework> containerConfiguration = new HashMap<>();
        Set<String> runningContainers = dockerInfo.getRunningContainers();

        for (String containerId : runningContainers) {
            String tempContainerId = containerId;
            String imageName = dockerInfo.getImageNameOfContainer(tempContainerId);

            DockerFramework framework = getFrameworkOfImage(imageName, frameworks);

            if (framework != null) {
                LOGGER.info(String.format("Found framework config for image %s", imageName));
                containerConfiguration.put(tempContainerId, framework);
            }

            LOGGER.info(String.format("Found no framework config for image %s", imageName));
        }

        return containerConfiguration;
    }

    private DockerFramework getFrameworkOfImage(String imageName, List<DockerFramework> frameworks) {
        for (DockerFramework f : frameworks) {
            if (imageName.equals(f.getName())) {
                return f;
            }
        }
        return null;
    }
}
