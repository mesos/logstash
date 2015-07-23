package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.LogStream;
import org.apache.mesos.logstash.executor.logging.LogStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Executors;

public class ByteBufferLogSteamWriter implements LogStreamWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferLogSteamWriter.class);

    private ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    private ByteArrayOutputStream stderr = new ByteArrayOutputStream();

    @Override
    public void write(String name, LogStream logStream) throws IOException {

        // FIXME: We should really stop all these gracefully on shutdown.
        logStream.attach(stdout, stderr);
    }

    public String getStdOutContent() throws UnsupportedEncodingException {
        return stdout.toString("UTF-8");
    }

    @SuppressWarnings("unused")
    public String getStdErrContent() throws UnsupportedEncodingException {
        return stderr.toString("UTF-8");
    }
}
