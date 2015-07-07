package org.apache.mesos.logstash.logging;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.frameworks.DockerFramework;
import org.apache.mesos.logstash.docker.DockerInfo;
import org.apache.mesos.logstash.LogstashConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ero on 01/07/15.
 */
public class LogfileStreaming {


    public static final Logger LOGGER = Logger.getLogger(LogstashConnector.class.toString());
    private final DockerInfo dockerInfo;
    private Map<String, String[]> logConfigurations;

    public LogfileStreaming(DockerInfo dockerInfo) {
        this.dockerInfo = dockerInfo;
        this.logConfigurations = new HashMap<>();
    }

    public void setupContainerLogfileStreaming(DockerFramework framework) {
        if(isConfigured(framework)) {
            LOGGER.info("Ignoring framework " + framework.getName() + " because it has already been configured");
            return;
        }
        LOGGER.info("Setting up log streaming for " + framework.getName());
        System.out.println("Setting up log streaming for " + framework.getName());

        ArrayList<String> localPaths = new ArrayList<>();

        for(String logLocation : framework.getLogLocations()) {
            String localPath = framework.getLocalLogLocation(logLocation);
            this.streamContainerLogFile(framework.getContainerId(), localPath, logLocation);
            localPaths.add(localPath);
        }

        logConfigurations.put(framework.getContainerId(), localPaths.toArray(new String[localPaths.size()]));
    }

    private boolean isConfigured(DockerFramework framework) {
        return this.logConfigurations.containsKey(framework.getContainerId());
    }

    /**
     * Start streaming the content of a log file within a docker container.
     *
     * @param fileName the fileName of the local file where logstash will read
     * @param logLocation the log file's location within the docker container
     */
    private void streamContainerLogFile(String containerId, String fileName, String logLocation) {
        LogDispatcher.writeLogToFile(fileName, createContainerLogStream(containerId, logLocation));

        LOGGER.info(String.format("Thread writing to file %s", fileName));
    }

    private com.spotify.docker.client.LogStream createContainerLogStream(String containerId, String logLocation) {
        final String MONITOR_CMD = String.format("touch %s; while sleep 3; do echo '%c HEARTBEAT'; done & tail -f %s", logLocation, LogDispatcher.MAGIC_CHARACTER, logLocation);

        LOGGER.info("Running command " + MONITOR_CMD);
        return dockerInfo.execInContainer(containerId, "bash", "-c", MONITOR_CMD);
    }
}
