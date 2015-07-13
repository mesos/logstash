package org.apache.mesos.logstash.scheduler;

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import javax.management.RuntimeErrorException;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Created by peldan on 01/07/15.
 */
public class ConfigMonitor {
    private Path configDir;

    public boolean isRunning() {
        return isRunning;
    }

    private boolean isRunning;

    public Thread getThread() {
        return thread;
    }

    private Thread thread;

    public static final Logger LOGGER = Logger.getLogger(Scheduler.class);


    public ConfigMonitor(String configDir) {
        this.configDir = FileSystems.getDefault().getPath(configDir);

        // Ensure directory exists
        this.configDir.toFile().mkdirs();
    }

    private static String readStringFromFile(File f) {
        try {
            return FileUtils.readFileToString(f);
        } catch (IOException e) {
            LOGGER.warn("Config file " + f.getAbsolutePath() + " couldn't be read");
            return "# Couldn't parse config " + f.getAbsolutePath();
        }
    }

    public void start(Consumer< Map<String, String> > onChange) {
        FutureTask<Boolean> isStarted = new FutureTask<>(() -> {}, true);


        LOGGER.info("Config monitor of " + this.configDir.toString() + " starting..");
        this.thread = new Thread(() -> {
            this.run(onChange, isStarted);
        });
        thread.start();


        try {
            isStarted.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Config monitor of " + this.configDir.toString() + " started");
    }

    private void run(Consumer< Map<String, String> > onChange, FutureTask<Boolean> isStarted) {
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


        WatchKey key = null;
        try {
            while (true) {
                isRunning = true;

                try {
                    key = watcher.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException x) {
                    return;
                }
                if(key == null) continue;

                boolean didChange = false;
                for (WatchEvent<?> event: key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == OVERFLOW) {
                        continue;
                    }

                    WatchEvent<Path> ev = (WatchEvent<Path>)event;
                    Path filename = ev.context();

                    String filenameString = filename.toString();
                    if(!filenameString.endsWith(".conf")) {
                        continue;
                    }
                    filenameString = filenameString.replace(".conf", "");


                    if(kind == ENTRY_DELETE) {
                        configToNameMap.remove(filenameString);
                        didChange = true;
                    }
                    else {
                        if(kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                            configToNameMap.put(filenameString, readStringFromFile(configDir.resolve(filename).toFile()));
                            didChange = true;
                        }
                    }
                }

                if(didChange) {
                    onChange.accept(configToNameMap);
                }

                if(key != null) {
                    if(!key.reset()) return;
                }
            }
        }
        finally {
            isRunning = false;
        }
    }
}
