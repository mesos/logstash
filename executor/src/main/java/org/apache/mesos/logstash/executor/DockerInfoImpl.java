package org.apache.mesos.logstash.executor;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import org.apache.log4j.Logger;


import javax.print.Doc;
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
    public Map<String, LogstashInfo> getContainersThatWantLogging() {
        try {
            List<Container> containers = getRunningContainers(this.dockerClient);
            List<ContainerInfo> containerInfos = getContainerResponses(dockerClient, containers);

            LOGGER.info(String.format("Found %d running containers", containers.size()));
            return parseLogstashInfoFromRunningContainers(containerInfos);

        } catch (DockerException e) {
            LOGGER.error(String.format("Error calling docker remote api %s", e));
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOGGER.error(String.format("Error calling docker remote api %s", e));
        }
        return null;
    }

    private List<Container> getRunningContainers(DockerClient dockerClient) throws DockerException, InterruptedException {
        return dockerClient.listContainers();
    }

    private List<ContainerInfo> getContainerResponses(DockerClient dockerClient, List<Container> containers) throws DockerException, InterruptedException {
        List<ContainerInfo> containerResponses = new ArrayList<>();
        for (Container container : containers) {
            containerResponses.add(dockerClient.inspectContainer(container.id()));
        }
        return containerResponses;
    }

    private Map<String, LogstashInfo> parseLogstashInfoFromRunningContainers(List<ContainerInfo> containers) {
        Map<String, LogstashInfo> runningContainers = new Hashtable<>();
        for (ContainerInfo container : containers) {
            LogstashInfo li = parseEnvironmentToLogstashInfo(container);
            if (li != null) {
                runningContainers.put(container.id(), li);
            }
        }
        LOGGER.info(String.format("Found %d CONFIGURED containers", runningContainers.size()));

        return runningContainers;
    }

    private LogstashInfo parseEnvironmentToLogstashInfo(ContainerInfo container) {
        String loggingLocationPath = null;
        String configurationPath = null;
        for (String env : container.config().env()) {
            if (loggingLocationPath == null) {
                loggingLocationPath = tryParseVariable(env, LOG_LOCATION);
            }
            if (configurationPath == null) {
                configurationPath = tryParseVariable(env, CONFIG_FILE);
            }
        }
        if (loggingLocationPath != null && configurationPath != null) {
            return new LogstashInfo(container.name(), loggingLocationPath, configurationPath);
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
        ContainerConfig containerConfig = ContainerConfig.builder()
                .image(imageId)
                .build();
        try {

            ContainerCreation containerCreation = dockerClient.createContainer(containerConfig);
            dockerClient.startContainer(containerCreation.id());
            return containerCreation.id();

        } catch (DockerException e) {
            LOGGER.error(String.format("Error starting container for image %s: %s", imageId, e));
        } catch (InterruptedException e) {
            LOGGER.error(String.format("Error starting container for image %s: %s", imageId, e));
        }
        return null;
    }

    public void stopContainer(String containerId) {
        try {
            dockerClient.stopContainer(containerId, 0);
        } catch (DockerException e) {
            LOGGER.error(String.format("Error stopping container %s: %s", containerId, e));
        } catch (InterruptedException e) {
            LOGGER.error(String.format("Error stopping container %s: %s", containerId, e));
        }
    }

    public LogStream execInContainer(String containerId, String... command) {

        try {
            String id = dockerClient.execCreate(containerId, command, DockerClient.ExecParameter.STDOUT,
                    DockerClient.ExecParameter.STDERR);
            LogStream logStream = dockerClient.execStart(id);
            return logStream;

        } catch (DockerException e) {
            LOGGER.error(String.format("Error executing in container %s: %s", containerId, e));
        } catch (InterruptedException e) {
            LOGGER.error(String.format("Error executing container %s: %s", containerId, e));
        }
        return null;
    }
}
