package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.Container;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Created by ero on 22/06/15.
 */
public class DockerInfoImpl implements DockerInfo {
    private final Logger LOGGER = Logger.getLogger(DockerInfoImpl.class.toString());
    private final String LOG_LOCATION = "LOG_LOCATION";
    private final String CONFIG_FILE = "CONFIG_FILE";

    private final DockerClient dockerClient;

    public DockerInfoImpl(DockerClient dockerClient) {
        this.dockerClient = dockerClient;
    }

    @Override
    public void attachEventListener(EventCallback eventCallback) {
        dockerClient.eventsCmd(eventCallback).exec();
    }

    @Override
    public Map<String, LogstashInfo> getContainersThatWantLogging() {
        List<Container> containers = getRunningContainers(dockerClient);
        List<InspectContainerResponse> containerResponses = getContainerResponses(dockerClient, containers);

        LOGGER.info(String.format("Found %d running containers", containers.size()));
        return parseLogstashInfoFromRunningContainers(containerResponses);
    }

    private List<Container> getRunningContainers(DockerClient dockerClient) {
        return dockerClient.listContainersCmd().exec();
    }

    private List<InspectContainerResponse> getContainerResponses (DockerClient dockerClient, List<Container> containers) {
        List<InspectContainerResponse> containerResponses = new ArrayList<>();
        for (Container container: containers) {
            containerResponses.add(dockerClient.inspectContainerCmd(container.getId()).exec());
        }
        return containerResponses;
    }

    private Map<String, LogstashInfo> parseLogstashInfoFromRunningContainers(List<InspectContainerResponse> containers) {
        Map<String, LogstashInfo> runningContainers = new Hashtable<>();
        for (InspectContainerResponse container : containers) {
            LogstashInfo li = parseEnvironmentToLogstashInfo(container);
            if (li != null) {
                runningContainers.put(container.getId(), li);
            }
        }
        LOGGER.info(String.format("Found %d CONFIGURED containers", runningContainers.size()));

        return runningContainers;
    }

    private LogstashInfo parseEnvironmentToLogstashInfo(InspectContainerResponse container) {
        String loggingLocationPath = null;
        String configurationPath = null;
        for (String env : container.getConfig().getEnv()) {
            if (loggingLocationPath == null) {
                loggingLocationPath = tryParseVariable(env, LOG_LOCATION);
            }
            if (configurationPath == null) {
                configurationPath = tryParseVariable(env, CONFIG_FILE);
            }
        }
        if (loggingLocationPath != null && configurationPath != null) {
            return new LogstashInfo(loggingLocationPath, configurationPath);
        }
        return null;
    }

    private String tryParseVariable(String env, String match) {
        String[] parts = env.split("=");
        if (parts[0].equals(match)) {
            return parts[1];
        }

        return null;
    }

    public String startContainer(String imageId) {
        CreateContainerResponse r = dockerClient.createContainerCmd(imageId).exec();

        dockerClient.startContainerCmd(r.getId()).exec();

        return r.getId();
    }

    public void stopContainer(String containerId) {
        dockerClient.stopContainerCmd(containerId);
    }

    public void execInContainer(String containerId, String... command) {
        String execId = dockerClient.execCreateCmd(containerId).withAttachStdout().withCmd(command).exec().getId();

        ExecStartCmd abc = dockerClient.execStartCmd(execId);
        InspectExecCmd cmd = dockerClient.inspectExecCmd(execId);

        try {
            abc.exec().close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER.info("COMMAND EXIT CODE: " + cmd.exec().getExitCode());
    }

}
