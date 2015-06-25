package org.apache.mesos.logstash.executor;

import java.io.*;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

/**
 * Created by ero on 24/06/15.
 */
public class LogDispatcher {
    public static final char MAGIC_CHARACTER = 'A';

    private static final String TEMP_PATH = "/tmp";

    private static final Logger LOGGER = Logger.getLogger(LogDispatcher.class.toString());

    public static String writeLogToFile(final String folder, final String fileName, final com.spotify.docker.client.LogStream logStream) {

        final Path path = Paths.get(TEMP_PATH, folder, fileName);

        LOGGER.info("Starting thread for reading logStream to file " + path.toString());

        ExecutorService executorService = Executors.newSingleThreadExecutor();


        executorService.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    FileOutputStream outputStream = new FileOutputStream(path.toFile(), true);
                    FilterOutputStream filtered = new HeartbeatFilterOutputStream(outputStream);

                    LOGGER.info("Reading stream...");
                    logStream.attach(filtered, filtered);


                } catch (FileNotFoundException e) {
                    LOGGER.error("Error writing to file " + e.toString());
                } catch (IOException e) {
                    LOGGER.error("Error reading stream " + e.toString());
                }
            }

        });

        return path.toString();
    }
}

