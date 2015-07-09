package org.apache.mesos.logstash.executor.docker;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.executor.ConfigManager;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.logging.FileLogSteamWriter;
import org.apache.mesos.logstash.executor.logging.LogStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.mesos.logstash.executor.logging.Constansts.MAGIC_CHARACTER;


public class DockerLogSteamManager {

    private static final Logger LOGGER = Logger.getLogger(ConfigManager.class.toString());
    private static final String BASH_COMMAND = "touch %s; while sleep 3; do echo '%c HEARTBEAT'; done & tail -F %s";

    private final ContainerizerClient containerizerClient;
    private final FileLogSteamWriter writer;
    private Map<String, String[]> logConfigurations;

    public DockerLogSteamManager(ContainerizerClient containerizerClient, FileLogSteamWriter writer) {
        this.containerizerClient = containerizerClient;
        this.writer = writer;
        this.logConfigurations = new HashMap<>();
    }

    public void setupContainerLogfileStreaming(DockerFramework framework) {

        if (isConfigured(framework)) {
            LOGGER.info("Ignoring framework " + framework.getName() + " because it has already been configured");
            return;
        }

        LOGGER.info("Setting up log streaming for " + framework.getName());
        System.out.println("Setting up log streaming for " + framework.getName());

        ArrayList<String> localPaths = new ArrayList<>();

        for (String pathInContainer : framework.getLogLocations()) {
            String pathInExecutor = framework.getLocalLogLocation(pathInContainer);

            try {
                streamContainerLogFile(framework.getContainerId(), pathInExecutor, pathInContainer);
            }
            catch (IOException e) {
                LOGGER.error("Failed to ");
            }

            localPaths.add(pathInExecutor);
        }

        logConfigurations.put(framework.getContainerId(), localPaths.toArray(new String[localPaths.size()]));
    }

    private boolean isConfigured(DockerFramework framework) {
        return this.logConfigurations.containsKey(framework.getContainerId());
    }

    /**
     * Start streaming the content of a log file within a docker container.
     *
     * @param fileName    the fileName of the local file where logstash will read
     * @param logLocation the log file's location within the docker container
     */
    private void streamContainerLogFile(String containerId, String fileName, String logLocation) throws IOException {
        LogStream logStream = createContainerLogStream(containerId, logLocation);
        writer.write(fileName, logStream);
        LOGGER.info(String.format("Logging file: %s", fileName));
    }

    private LogStream createContainerLogStream(String containerId, String logLocation) {
        String MONITOR_CMD = String.format(BASH_COMMAND, logLocation, MAGIC_CHARACTER, logLocation);
        LOGGER.info("Running: " + MONITOR_CMD);
        return containerizerClient.exec(containerId, "bash", "-c", MONITOR_CMD);
    }
}
