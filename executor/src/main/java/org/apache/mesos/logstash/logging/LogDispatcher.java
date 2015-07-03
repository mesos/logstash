package org.apache.mesos.logstash.logging;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

/**
 * Created by ero on 24/06/15.
 */
public class LogDispatcher {
    public static final char MAGIC_CHARACTER = '\u0002';

    private static final Logger LOGGER = Logger.getLogger(LogDispatcher.class.toString());

    public static void writeLogToFile(final String fileName, final com.spotify.docker.client.LogStream logStream) {

        final Path path = Paths.get(fileName);

        touch(path.toFile());

        LOGGER.info("Starting thread for reading logStream to file " + path.toString());

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    FileOutputStream outputStream = new FileOutputStream(path.toFile(), true);
                    FilterOutputStream filtered = new HeartbeatFilterOutputStream(outputStream);

                    LOGGER.info("Reading stream...");
                    logStream.attach(filtered, new ByteArrayOutputStream());

                } catch (FileNotFoundException e) {
                    LOGGER.error("Error writing to file", e);
                } catch (IOException e) {
                    LOGGER.error("Error reading stream", e);
                } catch (Exception e) {
                    LOGGER.error("Unexpected error", e);
                }
            }

        });
    }

    private static void touch(File file) {
        try {
            file.getParentFile().mkdirs();
            new FileOutputStream(file).close();
        }
        catch(IOException e) {
            LOGGER.error(e);
        }
    }
}

