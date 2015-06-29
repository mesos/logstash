package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * Class responsible for connecting each discovered framework to logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashConnector implements LogConfigurationListener {
    public static final Logger LOGGER = Logger.getLogger(LogstashConnector.class.toString());

    private LogstashService logstash;

    private DockerInfo dockerInfo;

    private Map<String, String[]> logConfigurations;

    public LogstashConnector(DockerInfo dockerInfo, LogstashService service) {
        this.dockerInfo = dockerInfo;
        this.logstash = service;
        this.logConfigurations = new HashMap<>();
    }

    public void init() {
        logstash.start();
    }


    private void setupContainerLogfileStreaming(String containerId, String[] logLocations) {
        if(logLocations.length == 0) {
            LOGGER.warn("No logs to configure for container " + containerId);
            return;
        }

        ArrayList<String> localPaths = new ArrayList<>();
        for(String logPath : logLocations) {
            final String localPath = this.streamContainerLogFile(containerId, logPath);
            localPaths.add(localPath);
        }
        logConfigurations.put(containerId, localPaths.toArray(new String[localPaths.size()]));
    }

    /**
     * Start streaming the content of a log file within a docker container.
     * @param containerId the docker container id
     * @param logLocation the log file's location within the docker container
     * @return the local path where the log file contents will be streamed to
     */
    private String streamContainerLogFile(String containerId, String logLocation) {
        final String fileName = LogDispatcher.writeLogToFile(containerId, "", createContainerLogStream(containerId, logLocation));

        LOGGER.info(String.format("Thread writing to file %s", fileName));
        return fileName;
    }

    private com.spotify.docker.client.LogStream createContainerLogStream(String containerId, String logLocation) {
        final String MONITOR_CMD = String.format("while sleep 3; do echo '%c HEARTBEAT'; done & tail -f %s", logLocation);

        LOGGER.info("Running command " + MONITOR_CMD);
        return dockerInfo.execInContainer(containerId, "bash", "-c", MONITOR_CMD);
    }

    @Override
    public void updatedLogLocations(Map<String, String[]> locationsPerImageName) {
        Set<String> running = dockerInfo.getRunningContainers();

        for(String containerId : running) {
            final String imageName = dockerInfo.getImageNameOfContainer(containerId);

            if(logConfigurations.containsKey(containerId)) {
                LOGGER.info(String.format("Skipping %s (%s) because it is already configured", containerId, imageName));
                continue;
            }
            if(!locationsPerImageName.containsKey(imageName)) {
                // We don't know where to locate the log files
                LOGGER.info(String.format("Ignoring running %s (%s) because missing log information", containerId, imageName));
                continue;
            }

            setupContainerLogfileStreaming(containerId, locationsPerImageName.get(imageName));
        }

        // TODO reconfigure
    }
}
