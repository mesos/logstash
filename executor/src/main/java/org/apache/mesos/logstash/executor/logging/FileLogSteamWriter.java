package org.apache.mesos.logstash.executor.logging;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class FileLogSteamWriter implements LogStreamWriter {

    private static final Logger LOGGER = Logger.getLogger(FileLogSteamWriter.class);

    @Override
    public void write(String name, LogStream logStream) throws IOException {

        Path path = Paths.get(name);

        FileUtils.touch(path.toFile());

        LOGGER.info("Starting thread for reading logStream to file " + path.toString());

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(() -> {

            // TODO: We could add more info about the stream.
            LOGGER.info("Reading stream...");

            try {
                FileOutputStream outputStream = new FileOutputStream(path.toFile(), true);
                // FIXME: How should we handle stderr?
                logStream.attach(outputStream, new ByteArrayOutputStream());
            } catch (IOException e) {
                LOGGER.error("Error writing to file", e);
            }
        });
    }
}

