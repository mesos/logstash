package org.apache.mesos.logstash.executor.docker;


import org.apache.mesos.logstash.executor.logging.LogStream;

import java.io.IOException;
import java.io.OutputStream;

public class DockerLogStream implements LogStream {

    private final com.spotify.docker.client.LogStream innerLogStream;

    public DockerLogStream(com.spotify.docker.client.LogStream innerLogStream) {
        this.innerLogStream = innerLogStream;
    }

    @Override
    public void attach(OutputStream stdout, OutputStream stderr) throws IOException {
        innerLogStream.attach(stdout, stderr);
    }
}
