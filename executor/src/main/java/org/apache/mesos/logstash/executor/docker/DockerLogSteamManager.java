package org.apache.mesos.logstash.executor.docker;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.executor.ConfigManager;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;

import java.util.HashSet;
import java.util.Set;

public class DockerLogSteamManager {

    private static final Logger LOGGER = Logger.getLogger(ConfigManager.class);

    private final Set<String> processedContainers;
    private final DockerStreamer streamer;

    public DockerLogSteamManager(DockerStreamer streamer) {
        this.streamer = streamer;

        this.processedContainers = new HashSet<>();
    }

    public void setupContainerLogfileStreaming(DockerFramework framework) {

        if (isAlreadySteaming(framework)) {
            LOGGER.info("Ignoring framework " + framework.getName()
                + " because it has already been configured");
            return;
        }

        LOGGER.info("Setting up log streaming for " + framework.getName());

        framework.getLogFiles().forEach(streamer::startStreaming);
        processedContainers.add(framework.getContainerId());

        LOGGER.info("Done processing: " + framework.getName());
    }

    public Set<String> getProcessedContainers() {
        return processedContainers;
    }

    private boolean isAlreadySteaming(DockerFramework framework) {
        return processedContainers.contains(framework.getContainerId());
    }

}
