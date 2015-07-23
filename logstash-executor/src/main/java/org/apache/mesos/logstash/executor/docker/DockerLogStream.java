package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.HeartbeatFilterOutputStream;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;


public class DockerLogStream implements LogStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerStreamer.class);
    private final com.spotify.docker.client.LogStream innerLogStream;

    private OutputStream stdout;
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
        HeartbeatFilterOutputStream heartbeatFilterOutputStream = new HeartbeatFilterOutputStream(stdout);
        this.stderr = stderr;
        this.stdout = heartbeatFilterOutputStream;

        innerLogStream.attach(heartbeatFilterOutputStream, stderr);
    }

    @Override public void close() {

        try {
            innerLogStream.close();
        } catch (RuntimeException e){
            LOGGER.error("Error while closing docker log stream", e);
        }

        try {
            stdout.close();
        } catch (IOException e) {
            LOGGER.error("Error while closing docker log stream stdout", e);
        }
        try {
            stderr.close();
        } catch (IOException e) {
            LOGGER.error("Error while closing docker log stream stderr", e);
        }

    }

}
