package org.apache.mesos.logstash.scheduler;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import static java.nio.file.StandardWatchEventKinds.*;

public class ConfigMonitor {

    public static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private final ScheduledExecutorService executorService;
    private final Path configDir;
    private final Map<String, String> configToNameMap;
    private final Collection<ConfigListener> listeners;

    public ConfigMonitor(String configDir) {
        this.configDir = FileSystems.getDefault().getPath(configDir);

        executorService = Executors.newSingleThreadScheduledExecutor();
        configToNameMap = Collections.synchronizedMap(new HashMap<>());
        listeners = Collections.synchronizedList(new ArrayList<>());
    }

    private static String readStringFromFile(File f) {
        try {
            return FileUtils.readFileToString(f);
        } catch (IOException e) {
            LOGGER.warn("Config file couldn't be read. path={}", f.getAbsolutePath());
            return "# Couldn't parse config " + f.getAbsolutePath();
        }
    }

    public void start() {

        // Ensure directory exists
        // TODO: Handle that the dir was not created.
        this.configDir.toFile().mkdirs();

        updateFiles();

        FutureTask<Boolean> isStarted = new FutureTask<>(() -> {}, true);

        LOGGER.info("Config monitor starting. dir={}", this.configDir);

        executorService.scheduleAtFixedRate(() -> this.run(isStarted), 0, 1, TimeUnit.SECONDS);

        // Wait for the watcher to run once before continuing.

        try {
            isStarted.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Config monitor started. dir={}", this.configDir);
    }

    private void run(FutureTask<Boolean> isStarted) {
        WatchService watcher;

        try {
            watcher = FileSystems.getDefault().newWatchService();
            configDir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            LOGGER.error("Failed to start monitoring config dir. dir={}", configDir, e);
            return;
        }

        notifyListeners();

        isStarted.run(); // Let creator know we have initialized successfully

        WatchKey key;

        while (true) {

            try {
                key = watcher.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException x) {
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
                }
                else {
                    if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                        configToNameMap.put(filenameString, readStringFromFile(configDir.resolve(filename).toFile()));
                        didChange = true;
                    }
                }
            }

            if (didChange) {
                notifyListeners();
            }

            if (!key.reset()) return;
        }
    }

    private void notifyListeners() {
        this.listeners.stream().forEach(l -> l.onNewConfig(this, configToNameMap));
    }

    private void updateFiles() {
        HashMap<String, String> configToNameMap = new HashMap<>();

        try {
            Files
                    .list(configDir)
                    .map(Path::toFile)
                    .filter(File::isFile)
                    .forEach(f -> configToNameMap.put(f.getName().replace(".conf", ""), readStringFromFile(f)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read files from config dir. dir=" + configDir, e);
        }
    }

    public void save(String name, String input) throws IOException {
        File file = FileUtils.getFile(this.configDir.toFile(), name + ".conf");
        FileUtils.write(file, input, false);

        // Don't wait until the monitor discovers it.
        configToNameMap.put(name, input);
        notifyListeners();
    }

    public void registerListener(ConfigListener listener) {
        listeners.add(listener);
    }
}
