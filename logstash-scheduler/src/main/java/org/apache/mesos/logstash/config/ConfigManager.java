package org.apache.mesos.logstash.config;

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.mesos.logstash.common.LogType;
import org.apache.mesos.logstash.scheduler.LogstashScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardWatchEventKinds.*;

@Component
public class ConfigManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private final LogstashScheduler scheduler;

    private final Path configRootDir;

    private final Map<String, String> dockerConfig;
    private final Map<String, String> hostConfig;
    private final ExecutorService executorService;

    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicInteger runningCount = new AtomicInteger(0);

    @Autowired
    public ConfigManager(LogstashScheduler scheduler, Path configRootDir) {
        this.scheduler = scheduler;
        this.configRootDir = configRootDir;
        dockerConfig = new HashMap<>();
        hostConfig = new HashMap<>();
        executorService = Executors.newFixedThreadPool(2);
    }

    @PostConstruct
    public void start() throws IOException {
        Files.createDirectories(getConfigDirectory(LogType.DOCKER));
        reloadConfigFiles(LogType.DOCKER);
        executorService.execute(() -> run(LogType.DOCKER));

        Files.createDirectories(getConfigDirectory(LogType.HOST));
        reloadConfigFiles(LogType.HOST);
        executorService.execute(() -> run(LogType.HOST));

        notifyScheduler();
    }

    @PreDestroy
    public void stop() {
        done.set(true);
    }

    // Both watchers can discover files - so we synchronize.
    private synchronized void notifyScheduler() {
        scheduler.configUpdated(dockerConfig, hostConfig);
    }

    private void run(LogType type) {
        WatchService watcher;
        Path path = getConfigDirectory(type);
        Map<String, String> config = getConfigMap(type);

        try {
            watcher = FileSystems.getDefault().newWatchService();
            WatchEvent.Kind<?>[] kinds = {ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY};
            path.register(watcher, kinds, SensitivityWatchEventModifier.HIGH);

        } catch (IOException e) {
            LOGGER.error("Failed to start monitoring config dir. Stopping. dir={}", path, e);
            return;
        }

        boolean hasReportedRunning = false;

        while (!done.get()) {

            WatchKey key;

            try {
                key = watcher.poll(500, TimeUnit.MILLISECONDS);
                if (!hasReportedRunning) {
                    hasReportedRunning = true;
                    runningCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Config Monitor interrupted. type={}", type);
                break;
            }

            // Check if we should shut down.
            if (key == null)
                continue;

            key.pollEvents().forEach(event -> {

                System.out.println("Event " + event.kind());

                WatchEvent.Kind<?> kind = event.kind();

                if (kind == OVERFLOW) {
                    // Since we might not have noticed a event occurs
                    // it make sense to re-read the entire directory again?
                    reloadConfigFiles(type);
                    notifyScheduler();
                    return;
                }

                // At this point we are sure it is a path event and
                // we can suppress the warning.
                @SuppressWarnings("unchecked")
                Path filePath = ((WatchEvent<Path>) event).context();
                filePath = path.resolve(filePath);

                if (!isConfigFile(filePath)) {
                    LOGGER.debug("Ignoring file in config directory. file={}", filePath);
                } else if (kind == ENTRY_DELETE) {
                    LOGGER.debug("Config file removed. file={}", filePath);
                    config.remove(entityName(filePath));
                    notifyScheduler();
                } else if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                    String content = readFile(filePath);
                    config.put(entityName(filePath), content);
                    LOGGER.info("Config file created/updated. event={}, file={}", kind, filePath);
                    notifyScheduler();
                } else {
                    LOGGER.debug("Config file event ignored. event={}, file={}", kind, filePath);
                }
            });

            // IMPORTANT: The key must be reset after being processed.
            if (!key.reset()) {
                LOGGER.error("Config Directory no longer observable. dir={}", path);
                break;
            }
        }

        LOGGER.info("Config Monitor Stopped. dir={}", path);
    }

    private Map<String, String> getConfigMap(LogType type) {
        return (type == LogType.DOCKER) ? dockerConfig : hostConfig;
    }

    private Path getConfigDirectory(LogType type) {
        return configRootDir.resolve(type.getFolder());
    }

    private boolean isConfigFile(Path path) {
        return path.toString().endsWith(".conf");
    }

    private String entityName(Path path) {
        return path.getFileName().toString().replace(".conf", "");
    }

    private Path configFilename(LogType type, String entityName) {
        return getConfigDirectory(type).resolve(entityName + ".conf");
    }

    private void reloadConfigFiles(LogType type) {
        Path dir = getConfigDirectory(type);

        try {
            Files.list(dir)
                .filter(Files::isRegularFile)
                .forEach(p -> getConfigMap(type).put(entityName(p), readFile(p)));
        } catch (IOException e) {
            // Not sure it is the best idea to throw a runtime exception here.
            // It may be acceptable to continue.
            throw new RuntimeException("Failed to read files from config dir. dir=" + dir, e);
        }
    }

    public void save(LogType type, String name, String input) throws IOException {
        Path path = configFilename(type, name);
        Files.write(path, input.getBytes());

        // Don't wait until the monitor discovers it in the file system.
        getConfigMap(type).put(name, input);

        LOGGER.info("Config file saved. file={}, type={}", path);
    }

    private String readFile(Path f) {
        try {
            return new Scanner(f.toFile()).useDelimiter("\\Z").next();
        } catch (IOException e) {
            LOGGER.warn("Config file couldn't be read. path={}", f.toAbsolutePath());
            return "# Couldn't parse config " + f.toAbsolutePath();
        }
    }

    public boolean isRunning() {
        return runningCount.get() == 2;
    }

    public Map<String, String> getDockerConfig() {
        return dockerConfig;
    }

    public Map<String, String> getHostConfig() {
        return hostConfig;
    }
}
