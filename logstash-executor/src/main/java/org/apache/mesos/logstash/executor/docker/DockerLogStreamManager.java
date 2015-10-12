package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.ConfigManager;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.apache.log4j.Logger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Processes the logs stream for Docker containers.
 */
public class DockerLogStreamManager {

    private static final Logger LOGGER = Logger.getLogger(ConfigManager.class.toString());

    final Map<String, Set<ProcessedDockerLogPath>> processedContainers;

    private final DockerStreamer streamer;

    public DockerLogStreamManager(DockerStreamer streamer) {
        this.streamer = streamer;

        this.processedContainers = new HashMap<>();
    }

    public synchronized void setupContainerLogfileStreaming(DockerFramework framework) {

        if (!isAlreadyStreaming(framework)) {
            processedContainers.put(framework.getContainerId(), new HashSet<>());
        }

        LOGGER.info("Setting up log streaming for " + framework.getName());

        streamUnprocessedLogFiles(framework);

        stopStreamingOfOrphanLogFiles(framework);

        LOGGER.info("Done processing: " + framework.getName());
    }

    public void stopStreamingForWholeFramework(String containerId){

        for (ProcessedDockerLogPath processedDockerLogPath : processedContainers.get(containerId)){
                LOGGER.info("Stop streaming of " + processedDockerLogPath.dockerLogPath);
                streamer.stopStreaming(containerId, processedDockerLogPath.logStream);
        }

        processedContainers.remove(containerId);
    }

    public Set<String> getProcessedContainers() {
        return processedContainers.keySet();
    }

    public Set<DockerLogPath> getProcessedFiles(String containerId) {
        if (processedContainers.containsKey(containerId)) {
            return processedContainers.
                get(containerId).stream()
                .map(ProcessedDockerLogPath::getDockerLogPath)
                .collect(Collectors.toSet());
        }
        return new HashSet<>();

    }

    private void streamUnprocessedLogFiles(DockerFramework framework) {
        List<DockerLogPath> frameWorkLogFiles = framework.getLogFiles();

        for (DockerLogPath dockerLogPath : frameWorkLogFiles){

            Set<ProcessedDockerLogPath> processedDockerLogPaths = processedContainers
                .get(framework.getContainerId());

            Set<DockerLogPath> currentDockerLogPaths = processedDockerLogPaths
                                                                .stream()
                                                                .map(ProcessedDockerLogPath::getDockerLogPath)
                                                                .collect(Collectors.toSet());

            if (!currentDockerLogPaths.contains(dockerLogPath)){
                LOGGER.info("Start streaming: " + dockerLogPath);
                LogStream logStream = streamer.startStreaming(dockerLogPath);
                processedDockerLogPaths.add(new ProcessedDockerLogPath(logStream, dockerLogPath));
            } else {
                LOGGER.info("Ignoring already streaming: " + dockerLogPath);
            }
        }
    }

    private boolean isAlreadyStreaming(DockerFramework framework) {
        return processedContainers.containsKey(framework.getContainerId());
    }

    private void stopStreamingOfOrphanLogFiles(DockerFramework framework) {
        List<DockerLogPath> frameWorkLogFiles = framework.getLogFiles();

        Iterator<ProcessedDockerLogPath> iterator = processedContainers.get(
            framework.getContainerId()).iterator();

        ProcessedDockerLogPath processedDockerLogPath;
        while (iterator.hasNext()){
            processedDockerLogPath = iterator.next();

            if (!frameWorkLogFiles.contains(processedDockerLogPath.dockerLogPath)){
                LOGGER.info("Stop streaming of " + processedDockerLogPath.dockerLogPath);
                streamer.stopStreaming(framework.getContainerId(), processedDockerLogPath.logStream);
                iterator.remove();
            }
        }
    }


    static class ProcessedDockerLogPath {
        final LogStream logStream;
        final DockerLogPath dockerLogPath;

        private ProcessedDockerLogPath(LogStream logStream, DockerLogPath dockerLogPath) {
            this.logStream = logStream;
            this.dockerLogPath = dockerLogPath;
        }


        public DockerLogPath getDockerLogPath() {
            return dockerLogPath;
        }
    }
}
