package org.apache.mesos.logstash.executor.logging;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Executors;

public class ByteBufferLogSteamWriter implements LogStreamWriter {

    private static final Logger LOGGER = Logger.getLogger(ByteBufferLogSteamWriter.class);

    private ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    private ByteArrayOutputStream stderr = new ByteArrayOutputStream();

    @Override
    public void write(String name, LogStream logStream) throws IOException {

        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                logStream.attach(stdout, stderr);
            } catch (IOException e) {
                LOGGER.error("Error writing to file", e);
            }
        });
    }

    public String getStdOutContent() throws UnsupportedEncodingException {
        return stdout.toString("UTF-8");
    }

    public String getStdErrContent() throws UnsupportedEncodingException {
        return stderr.toString("UTF-8");
    }
}
