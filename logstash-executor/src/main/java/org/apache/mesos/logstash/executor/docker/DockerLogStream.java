package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.LogStream;
import org.apache.mesos.logstash.executor.logging.LogstashPidFilterOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;


public class DockerLogStream implements LogStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerStreamer.class);
    private final com.spotify.docker.client.LogStream innerLogStream;

    private LogstashPidFilterOutputStream stdout;
    private OutputStream stderr;

    public DockerLogStream(com.spotify.docker.client.LogStream innerLogStream) {
        this.innerLogStream = innerLogStream;
    }



    @Override
    public synchronized void attach(OutputStream stdout, OutputStream stderr) throws IOException {

        if (this.stdout != null || this.stderr != null){
            throw new IllegalStateException("To use an allready attached DockerLogStream is not supported");
        }

        // Filter out heartbeats that keep the socket alive.
        LogstashPidFilterOutputStream logstashPidFilterOutputStream = new LogstashPidFilterOutputStream(stdout);
        this.stderr = stderr;
        this.stdout = logstashPidFilterOutputStream;

        innerLogStream.attach(logstashPidFilterOutputStream, stderr);
    }

    @Override public String readFully() {
        return innerLogStream.readFully();
    }

    @Override public void close() {

        try {
            stdout.close();
        } catch (Exception e) {
            LOGGER.error("Error while closing docker log stream stdout", e);
        }
        try {
            stderr.close();
        } catch (Exception e) {
            LOGGER.error("Error while closing docker log stream stderr", e);
        }

    }

    @Override public String getLogstashPid() {
        return stdout.pid;
    }

}
