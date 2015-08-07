package org.apache.mesos.logstash.executor.logging;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileLogSteamWriter implements LogStreamWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileLogSteamWriter.class);
    private final long maxLogSize;

    public FileLogSteamWriter(long maxLogSize) {
        this.maxLogSize = maxLogSize;
    }

    @Override
    public void write(String name, LogStream logStream) throws IOException {

        Path path = Paths.get(name); // TODO what happens if name contains invalid characters?

        FileUtils.touch(path.toFile());

        LOGGER.info("Starting thread for reading logStream to file " + path.toString());

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(() -> {

            // TODO: We could add more info about the stream.
            LOGGER.info("Reading stream...");

            try {

                OutputStream myOutput = new OutputStream() {
                    FileOutputStream outputStream = new FileOutputStream(path.toFile(), false);
                    int numBytesWritten;

                    @Override
                    public void write(int b) throws IOException {
                        getEndpoint().write(b);
                        numBytesWritten++;
                        recalculateEndpoint();
                    }

                    OutputStream getEndpoint() {
                        return outputStream;
                    }

                    void recalculateEndpoint() {
                        if (numBytesWritten >= maxLogSize) {
                            numBytesWritten = 0;
                            try {
                                outputStream.close();
                                path.toFile().delete();
                                outputStream = new FileOutputStream(path.toFile(), false);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    @Override
                    public void flush() throws IOException {
                        getEndpoint().flush();
                    }
                };

                // Note: the only thing we stream in logstash-mesos is the output of 'tail -F', so
                // we don't care that much about standard error
                logStream.attach(myOutput, new OutputStream() {
                    @Override public void write(int b) throws IOException {
                        //noop
                    }
                });

                LOGGER.debug("THREAD finished ");
            } catch (IOException e) {
                LOGGER.error("Error writing to file", e);
            }
        });
    }
}

