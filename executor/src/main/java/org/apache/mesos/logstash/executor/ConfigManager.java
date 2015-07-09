package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.frameworks.Framework;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.apache.mesos.logstash.executor.frameworks.HostFramework;
import org.apache.mesos.logstash.executor.docker.DockerLogSteamManager;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.mesos.logstash.executor.LogType.DOCKER;
import static org.apache.mesos.logstash.executor.LogType.HOST;

/**
 * Class responsible for connecting each discovered framework to manager
 */
public class ConfigManager implements ConfigEventListener {

    public static final Logger LOGGER = Logger.getLogger(ConfigManager.class.toString());

    private LogstashManager manager;
    private ContainerizerClient containerizerClient;
    private DockerLogSteamManager dockerLogSteamManager;
    private List<FrameworkInfo> cachedDockerInfos;

    public ConfigManager(ContainerizerClient containerizerClient, LogstashManager manager, DockerLogSteamManager dockerLogSteamManager) {
        this.containerizerClient = containerizerClient;
        this.manager = manager;
        this.dockerLogSteamManager = dockerLogSteamManager;
        this.containerizerClient.setDelegate(_images -> {
            LOGGER.info("New containers discovered. Reconfiguring.");
            reconfigureDockerLogs(cachedDockerInfos.stream());
        });
    }

    private void reconfigureDockerLogs(Stream<FrameworkInfo> logstashInfos) {
        Function<String, FrameworkInfo> lookupConfig = createLookupHelper(logstashInfos);

        // Create frameworks
        Stream<DockerFramework> frameworks = containerizerClient.getRunningContainers().stream()
                .filter(c -> lookupConfig.apply(c) != null)
                .map(c -> new DockerFramework(lookupConfig.apply(c), new DockerFramework.ContainerId(c)));

        // Make sure all new containers are streaming their logs
        Stream<DockerFramework> frameworks2 = frameworks.peek(dockerLogSteamManager::setupContainerLogfileStreaming);

        // Generate configs
        String config = frameworks2
                .map(Framework::getConfiguration)
                .collect(Collectors.joining("\n"));

        manager.updateConfig(DOCKER, config);
    }

    @Override
    public void onConfigUpdated(LogType type, Stream<FrameworkInfo> info) {
        switch(type) {
            case HOST: updateHost(info); break;
            case DOCKER: updateDocker(info); break;
        }
    }

    private void updateHost(Stream<FrameworkInfo> logstashInfos) {
        String config = logstashInfos.map(HostFramework::new)
                .map(Framework::getConfiguration)
                .collect(Collectors.joining("\n"));

        // Dummy input to keep logstash alive in case there is no config available
        final String DUMMY_INPUT = "\ninput { file { path => '/dev/null' } }\n";
        manager.updateConfig(HOST, config + DUMMY_INPUT);
    }

    private void updateDocker(Stream<FrameworkInfo> logstashInfos) {
        // Make a local copy so that we can immediately reconfigure if we discover new containers!
        cachedDockerInfos = logstashInfos.collect(Collectors.toList());
        reconfigureDockerLogs(cachedDockerInfos.stream());
    }

    private Function<String, FrameworkInfo> createLookupHelper(Stream<FrameworkInfo> logstashInfos) {
        // TODO we need to make this helper more generic, so that a prefix/suffix match is also valid
        // i.e if the config name is a prefix or suffix of a containers image name
        Map<String, FrameworkInfo> logstashInfoMap = logstashInfos.collect(Collectors.toMap(FrameworkInfo::getName, x -> x));
        return c -> logstashInfoMap.get(containerizerClient.getImageNameOfContainer(c));
    }
}
