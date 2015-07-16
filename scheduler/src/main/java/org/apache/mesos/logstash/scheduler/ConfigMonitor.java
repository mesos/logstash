package org.apache.mesos.logstash.scheduler;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.*;

public class ConfigMonitor {

    public static final Logger LOGGER = Logger.getLogger(Scheduler.class);

    private Path configDir;

    @Autowired
    public ConfigMonitor(@Qualifier("configDir") String configDir) {
        this.configDir = FileSystems.getDefault().getPath(configDir);
    }

    private static String readStringFromFile(File f) {
        try {
            return FileUtils.readFileToString(f);
        } catch (IOException e) {
            LOGGER.warn("Config file " + f.getAbsolutePath() + " couldn't be read");
            return "# Couldn't parse config " + f.getAbsolutePath();
        }
    }

    public void start(Consumer<Map<String, String>> onChange) {

        // Ensure directory exists
        this.configDir.toFile().mkdirs();

        FutureTask<Boolean> isStarted = new FutureTask<>(() -> {
        }, true);

        LOGGER.info("Config monitor of " + this.configDir.toString() + " starting..");

        // TODO: This thread is never properly stopped. Does not really matter if the JVM is killed, but we should clean up.
        Thread thread = new Thread(() -> {
            this.run(onChange, isStarted);
        });

        thread.start();

        try {
            isStarted.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Config monitor of " + this.configDir.toString() + " started");
    }

    private void run(Consumer<Map<String, String>> onChange, FutureTask<Boolean> isStarted) {
        WatchService watcher;

        try {
            watcher = FileSystems.getDefault().newWatchService();
            configDir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            LOGGER.error(e);
            return;
        }

        HashMap<String, String> configToNameMap = new HashMap<>();
        try {
            Files
                    .list(configDir)
                    .map(Path::toFile)
                    .filter(File::isFile)
                    .forEach(f -> configToNameMap.put(f.getName().replace(".conf", ""), readStringFromFile(f)));
        } catch (IOException e) {
            LOGGER.error(e);
            return;
        }

        onChange.accept(configToNameMap);

        isStarted.run(); // Let creator know we have initialized successfully


        WatchKey key;
        while (true) {

            try {
                key = watcher.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException x) {
                // TODO: Log this event
                return;
            }

            if (key == null) continue;

            boolean didChange = false;
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == OVERFLOW) {
                    continue;
                }

                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();

                String filenameString = filename.toString();
                if (!filenameString.endsWith(".conf")) {
                    continue;
                }

                filenameString = filenameString.replace(".conf", "");

                if (kind == ENTRY_DELETE) {
                    configToNameMap.remove(filenameString);
                    didChange = true;
                } else {
                    if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                        configToNameMap.put(filenameString, readStringFromFile(configDir.resolve(filename).toFile()));
                        didChange = true;
                    }
                }
            }

            if (didChange) {
                onChange.accept(configToNameMap);
            }

            if (!key.reset()) return;
        }
    }

    public void save(String name, String input) throws IOException {
        File file = FileUtils.getFile(this.configDir.toFile(), name + ".conf");
        FileUtils.write(file, input, false);
    }
}
