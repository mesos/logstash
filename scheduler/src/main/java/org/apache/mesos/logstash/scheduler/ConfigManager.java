package org.apache.mesos.logstash.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Collections.synchronizedMap;
import static java.util.Collections.unmodifiableMap;

@Component
public class ConfigManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    private final ConfigMonitor dockerMonitor;
    private final ConfigMonitor hostMonitor;

    // one-to-one map of slave and corresponding executor
    private Map<String, String> dockerConfig;
    private Map<String, String> hostConfig;

    private Collection<ConfigEventListener> listeners;

    @Autowired
    public ConfigManager(
            @Qualifier("docker") ConfigMonitor dockerMonitor,
            @Qualifier("host") ConfigMonitor hostMonitor) {

        this.dockerMonitor = dockerMonitor;
        this.hostMonitor = hostMonitor;

        listeners = Collections.synchronizedCollection(new ArrayList<>());
        dockerConfig = synchronizedMap(new HashMap<>());
        hostConfig = synchronizedMap(new HashMap<>());
    }


    @PostConstruct
    public void start() {
        dockerMonitor.start(this::newDockerConfigAvailable);
        hostMonitor.start(this::newHostConfigAvailable);
    }


    public void registerListener(ConfigEventListener listener) {
        listeners.add(listener);
    }


    private void newDockerConfigAvailable(Map<String, String> config) {
        synchronized (this) {
            dockerConfig.clear();
            dockerConfig.putAll(config);
            this.broadcastConfig();
        }
    }


    private void newHostConfigAvailable(Map<String, String> config) {
        synchronized (this) {
            hostConfig.clear();
            hostConfig.putAll(config);
            this.broadcastConfig();
        }
    }

    private void broadcastConfig() {
        if (dockerConfig == null || hostConfig == null) {
            LOGGER.info("Skipping broadcast, haven't read all configs yet");
            return;
        }

        ConfigPair config = new ConfigPair(dockerConfig, hostConfig);
        listeners.stream().forEach(listener -> listener.configUpdated(config));
    }

    public ConfigPair getConfig() {
        Map<String, String> dockerConfigCopy = new HashMap<>();
        Map<String, String> hostConfigCopy = new HashMap<>();
        synchronized (this) {
            dockerConfigCopy.putAll(dockerConfig);
            hostConfigCopy.putAll(hostConfig);
        }
        return new ConfigPair(dockerConfigCopy, hostConfigCopy);
    }


    public static class ConfigPair {

        private Map<String, String> dockerConfig;
        private Map<String, String> hostConfig;

        public ConfigPair(Map<String, String> dockerConfig, Map<String, String> hostConfig) {
            this.dockerConfig = unmodifiableMap(dockerConfig);
            this.hostConfig = unmodifiableMap(hostConfig);
        }

        public Map<String, String> getDockerConfig() {
            return dockerConfig;
        }

        public Map<String, String> getHostConfig() {
            return hostConfig;
        }
    }
}
