package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.LogStream;
import org.apache.mesos.logstash.executor.logging.LogstashPidFilterOutputStream;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Represents the output stream from a docker exec command.
 *
 *  TODO: remove this class put the logic to whom who uses attach
 */
public class DockerLogStream implements LogStream {
    private static final Logger LOGGER = Logger.getLogger(DockerStreamer.class.toString());
    private final com.spotify.docker.client.LogStream innerLogStream;

    private LogstashPidFilterOutputStream logstashPidFilterOutputStream;
    private OutputStream stderr;

    public DockerLogStream(com.spotify.docker.client.LogStream innerLogStream) {
        this.innerLogStream = innerLogStream;
    }



    @Override
    public synchronized void attach(OutputStream stdout, OutputStream stderr) throws IOException {

        if (this.logstashPidFilterOutputStream != null || this.stderr != null){
            throw new IllegalStateException("To use an allready attached DockerLogStream is not supported");
        }

        // Filter out heartbeats that keep the socket alive.
        LogstashPidFilterOutputStream logstashPidFilterOutputStream = new LogstashPidFilterOutputStream(stdout);
        this.stderr = stderr;
        this.logstashPidFilterOutputStream = logstashPidFilterOutputStream;

        innerLogStream.attach(logstashPidFilterOutputStream, stderr);
    }

    @Override public String readFully() {
        return innerLogStream.readFully();
    }

    @Override public void close() {

        try {
            logstashPidFilterOutputStream.close();
        } catch (Exception e) {
            LOGGER.error("Error while closing docker log stream logstashPidFilterOutputStream", e);
        }
        try {
            stderr.close();
        } catch (Exception e) {
            LOGGER.error("Error while closing docker log stream stderr", e);
        }

    }

    @Override public String getLogstashPid() {
        return logstashPidFilterOutputStream.getPid();
    }

}
