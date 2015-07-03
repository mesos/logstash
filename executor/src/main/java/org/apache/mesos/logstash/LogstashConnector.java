package org.apache.mesos.logstash;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.docker.DockerInfo;
import org.apache.mesos.logstash.frameworks.DockerFramework;
import org.apache.mesos.logstash.frameworks.Framework;
import org.apache.mesos.logstash.frameworks.HostFramework;
import org.apache.mesos.logstash.logging.LogfileStreaming;

import java.util.*;
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

        Map<String, LogstashInfo> logstashInfoMap = logstashInfos.collect(Collectors.toMap(LogstashInfo::getName, x -> x));
        Stream<DockerFramework> frameworks = dockerInfo.getRunningContainers().stream()
                .map(c -> createDockerFramework(logstashInfoMap, c));

        // Make sure all new containers are streaming their logs
        frameworks.peek(fw -> this.logfileStreaming.setupContainerLogfileStreaming(fw));

        String config = frameworks
                .map(Framework::generateLogstashConfig)
                .collect(Collectors.joining("\n"));

        logstashService.updateDockerConfig(config);
    }

    private DockerFramework createDockerFramework(Map<String, LogstashInfo> logstashInfoMap, String containerId) {
        return new DockerFramework(logstashInfoMap.get(dockerInfo.getImageNameOfContainer(containerId)),
                new DockerFramework.ContainerId(containerId));
    }
}
