package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.LogStream;
import org.apache.mesos.logstash.executor.logging.LogStreamWriter;
import org.apache.mesos.logstash.executor.logging.LogstashPidFilterOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DockerStreamer {

    private static final String BASH_START_LOGSTASH_COMMAND = "touch %s; tail -F %s & echo %s$!";
    private static final String BASH_STOP_LOGSTASH_COMMAND = "kill %s";
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerStreamer.class);

    private final LogStreamWriter writer;
    private final ContainerizerClient client;

    public DockerStreamer(LogStreamWriter writer, ContainerizerClient client) {

        this.writer = writer;
        this.client = client;
    }

    public LogStream startStreaming(DockerLogPath path) {
        LOGGER
            .info("Stream from " + path.getContainerLogPath() + " to " + path.getExecutorLogPath());

        LogStream logStream = createContainerLogStream(path.getContainerId(),
            path.getContainerLogPath());
        try {
            writer.write(path.getExecutorLogPath(), logStream);

            LOGGER.info(String.format("Logging file: %s", path.getExecutorLogPath()));

            return logStream;

        } catch (IOException e) {
            LOGGER.error("Failed", e);
            logStream.close();
            return null;
        }
    }

    private LogStream createContainerLogStream(String containerId, String logLocation) {
        String MONITOR_CMD = getMonitorCmd(logLocation);
        LOGGER.info("Running: " + MONITOR_CMD);
        return client.exec(containerId, "sh", "-c", MONITOR_CMD);
    }

    String getMonitorCmd(String logLocation) {
        return String
            .format(BASH_START_LOGSTASH_COMMAND, logLocation, logLocation,
                LogstashPidFilterOutputStream.MAGIC_CHARACTER);
    }

    String getKillLogstashCmd(String pid) {
        return String
            .format(BASH_STOP_LOGSTASH_COMMAND, pid);
    }

    public void stopStreaming(String containerId, LogStream logStream) {
        String logstashPid = logStream.getLogstashPid();
        String killLogstashCmd = getKillLogstashCmd(logstashPid);

        LOGGER.debug("Killing logstash process in container {} - logstash pid {}", containerId, logstashPid);

        // we need to kill the logstash process within the container to stop the docker exec streaming
        LogStream ls = client
            .exec(containerId, "sh", "-c", killLogstashCmd);
        ls.readFully();

        LOGGER.debug("Finished killing logstash process in container {} and logstash pid {}",
            containerId, logstashPid);

        logStream.close();
    }
}