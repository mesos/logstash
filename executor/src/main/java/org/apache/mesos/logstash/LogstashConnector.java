package org.apache.mesos.logstash;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.docker.DockerInfo;
import org.apache.mesos.logstash.frameworks.DockerFramework;
import org.apache.mesos.logstash.frameworks.Framework;
import org.apache.mesos.logstash.frameworks.HostFramework;
import org.apache.mesos.logstash.logging.LogfileStreaming;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class responsible for connecting each discovered framework to logstashService
 * Created by peldan on 22/06/15.
 */
public class LogstashConnector implements LogConfigurationListener {
    public static final Logger LOGGER = Logger.getLogger(LogstashConnector.class.toString());

    private LogstashService logstashService;

    private DockerInfo dockerInfo;

    private LogfileStreaming logfileStreaming;

    public LogstashConnector(DockerInfo dockerInfo, LogstashService logstashService, LogfileStreaming logfileStreaming) {
        this.dockerInfo = dockerInfo;
        this.logstashService = logstashService;
        this.logfileStreaming = logfileStreaming;
    }

    @Override
    public void updatedHostLogConfigurations(Stream<LogstashInfo> logstashInfos) {
        String config = logstashInfos.map(lif -> new HostFramework(lif))
                .map(Framework::generateLogstashConfig)
                .collect(Collectors.joining("\n"));

        logstashService.updateStaticConfig(config);
    }


    @Override
    public void updatedDockerLogConfigurations(Stream<LogstashInfo> logstashInfos) {
        Function<String, LogstashInfo> lookupConfig = createLookupHelper(logstashInfos);

        // Create frameworks
        Stream<DockerFramework> frameworks = dockerInfo.getRunningContainers().stream()
                .filter(c -> lookupConfig.apply(c) != null)
                .map(c -> new DockerFramework(lookupConfig.apply(c), new DockerFramework.ContainerId(c)));

        // Make sure all new containers are streaming their logs
        Stream<DockerFramework> frameworks2 = frameworks.peek(fw -> this.logfileStreaming.setupContainerLogfileStreaming(fw));

        // Generate configs
        String config = frameworks2
                .map(Framework::generateLogstashConfig)
                .collect(Collectors.joining("\n"));

        logstashService.updateDockerConfig(config);
    }

    private Function<String, LogstashInfo> createLookupHelper(Stream<LogstashInfo> logstashInfos) {
        Map<String, LogstashInfo> logstashInfoMap = logstashInfos.collect(Collectors.toMap(LogstashInfo::getName, x -> x));
        return c -> logstashInfoMap.get(dockerInfo.getImageNameOfContainer(c));
    }
}
