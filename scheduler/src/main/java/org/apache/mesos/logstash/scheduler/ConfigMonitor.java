package org.apache.mesos.logstash.scheduler;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Created by peldan on 01/07/15.
 */
public class ConfigMonitor {
    private Path configDir;
    private boolean isRunning;

    public static final Logger LOGGER = Logger.getLogger(Scheduler.class);


    public ConfigMonitor(String configDir) {
        this.configDir = FileSystems.getDefault().getPath(configDir);
    }

    private static String readStringFromFile(File f) {
        try {
            FileUtils.readFileToString(f);
        } catch (IOException e) {
            LOGGER.warn("Config file " + f.getAbsolutePath() + " couldn't be read");
            return "# Couldn't parse config " + f.getAbsolutePath();
        }
    }

    public void start(Consumer< Map<String, String> > onChange) {
        new Thread(() -> this.run(onChange));
    }

    public void run(Consumer< Map<String, String> > onChange) {
        WatchService watcher;

        try {
            watcher = FileSystems.getDefault().newWatchService();
            configDir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            LOGGER.error(e);
            return;
        }

        WatchKey key;
        while (true) {
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Map<String, String> configToNameMap = new HashMap<>();
            try {
                Files
                        .list(configDir)
                        .map(Path::toFile)
                        .filter(File::isDirectory)
                        .forEach(f -> configToNameMap.put(f.getName(), readStringFromFile(f)));
            } catch (IOException e) {
                LOGGER.error(e);
                return;
            }

            onChange.accept(configToNameMap);
        }
    }
}
