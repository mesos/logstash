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

    private List<LogstashInfo> cachedDockerInfos;

    public LogstashConnector(DockerInfo dockerInfo, LogstashService logstashService, LogfileStreaming logfileStreaming) {
        this.dockerInfo = dockerInfo;
        this.logstashService = logstashService;
        this.logfileStreaming = logfileStreaming;
        this.dockerInfo.setContainerDiscoveryConsumer(_images -> {
            LOGGER.info("New containers discovered. Reconfiguring");
            System.out.println("New containers discovered. Reconfiguring");
            reconfigureDockerLogs(cachedDockerInfos.stream());
        });
    }

    private void reconfigureDockerLogs(Stream<LogstashInfo> logstashInfos) {
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

    @Override
    public void updatedHostLogConfigurations(Stream<LogstashInfo> logstashInfos) {
        String config = logstashInfos.map(lif -> new HostFramework(lif))
                .map(Framework::generateLogstashConfig)
                .collect(Collectors.joining("\n"));

        // Dummy input to keep logstash alive in case there is no config available
        final String DUMMY_INPUT = "\ninput { file { path => '/dev/null' } }\n";
        logstashService.updateStaticConfig(config + DUMMY_INPUT);
    }


    @Override
    public void updatedDockerLogConfigurations(Stream<LogstashInfo> logstashInfos) {

        // Make a local copy so that we can immediately reconfigure if we discover new containers!
        cachedDockerInfos = logstashInfos.collect(Collectors.toList());

        reconfigureDockerLogs(cachedDockerInfos.stream());
    }

    private Function<String, LogstashInfo> createLookupHelper(Stream<LogstashInfo> logstashInfos) {
        // TODO we need to make this helper more generic, so that a prefix/suffix match is also valid
        // i.e if the config name is a prefix or suffix of a containers image name
        Map<String, LogstashInfo> logstashInfoMap = logstashInfos.collect(Collectors.toMap(LogstashInfo::getName, x -> x));
        return c -> logstashInfoMap.get(dockerInfo.getImageNameOfContainer(c));
    }
}
