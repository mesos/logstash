package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.LogStream;
import org.apache.mesos.logstash.executor.logging.LogStreamWriter;
import org.apache.mesos.logstash.executor.logging.LogstashPidFilterOutputStream;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Streams Docker logs.
 */
public class DockerStreamer {

    private static final String BASH_START_LOGSTASH_COMMAND = "touch %s; tail -F %s & echo %s$!";
    private static final String BASH_STOP_LOGSTASH_COMMAND = "kill %s";
    private static final Logger LOGGER = Logger.getLogger(DockerStreamer.class.toString());

    private final LogStreamWriter writer;
    private final DockerClient client;

    public DockerStreamer(LogStreamWriter writer, DockerClient client) {

        this.writer = writer;
        this.client = client;
    }

    public LogStream startStreaming(DockerLogPath path) {
        LOGGER.info("Stream from " + path.getContainerLogPath() + " to " + path.getExecutorLogPath());

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
        String monitorCommand = getMonitorCmd(logLocation);
        LOGGER.info("Running: " + monitorCommand);
        return client.exec(containerId, "sh", "-c", monitorCommand);
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

        LOGGER.debug(String.format("Killing logstash process in container %s - logstash pid %s", containerId,
		      logstashPid));

        // TODO (Florian) ask config manage whether the container is running and only if so stop docker exec...


        if (client.getRunningContainers().contains(containerId)) {
            // we need to kill the logstash process within the container to stop the docker exec streaming
            LogStream ls = client
                .exec(containerId, "sh", "-c", killLogstashCmd);
            ls.readFully();

            LOGGER.debug(String.format("Finished killing logstash process in container %s and logstash pid %s",
			  containerId, logstashPid));
        }
        logStream.close();
    }
}
