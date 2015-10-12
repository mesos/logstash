package org.apache.mesos.logstash.config;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.state.FrameworkState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Component
public class ConfigManager {
    private static final Logger LOGGER = Logger.getLogger(ConfigManager.class.toString());

    private final Map<String, LogstashProtos.LogstashConfig> configCache;
    private FrameworkState frameworkState;

    private boolean isRunning;

    private Consumer<List<LogstashProtos.LogstashConfig>> onConfigUpdate;

    @Autowired
    public ConfigManager(Configuration configuration) {
        this.frameworkState = configuration.getFrameworkState();
        this.isRunning = false;
        configCache = Collections.synchronizedMap(new HashMap<>());
    }

    @PostConstruct
    public void start()
        throws ExecutionException, InterruptedException, InvalidProtocolBufferException {

        LogstashProtos.SchedulerMessage persistedConfig = frameworkState
            .getLatestConfig();

        LOGGER.info("Fetched latest config: " + persistedConfig);

        List<LogstashProtos.LogstashConfig> configs = (persistedConfig != null) ? persistedConfig.getConfigsList() : new ArrayList<>();
        configs.stream().forEach(c -> configCache.put(c.getFrameworkName(), c));

        isRunning = true;
        notifyScheduler();
    }

    // Both watchers can discover files - so we synchronize.
    private void notifyScheduler() {
        if (onConfigUpdate != null) {
            onConfigUpdate.accept(getLatestConfig());
        }
    }

    public void save(LogstashProtos.LogstashConfig config)
        throws IOException, ExecutionException, InterruptedException {
        configCache.put(config.getFrameworkName(), config);
        frameworkState.setLatestConfig(getLatestConfig());
        notifyScheduler();
    }

    public List<LogstashProtos.LogstashConfig> getLatestConfig() {
        return Collections.unmodifiableList(new ArrayList<>(configCache.values()));
    }

    public boolean isRunning() {
        return this.isRunning;
    }

    public void setOnConfigUpdate(
        Consumer<List<LogstashProtos.LogstashConfig>> onConfigUpdate) {
        this.onConfigUpdate = onConfigUpdate;
    }

    public void delete(String name) throws ExecutionException, InterruptedException {
        configCache.remove(name);
        frameworkState.setLatestConfig(getLatestConfig());
        notifyScheduler();
    }
}
