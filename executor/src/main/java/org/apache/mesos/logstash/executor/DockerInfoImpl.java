package org.apache.mesos.logstash.executor;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerCreation;
import org.apache.log4j.Logger;


import java.util.*;

/**
 * Created by ero on 22/06/15.
 */
public class DockerInfoImpl implements DockerInfo {

    private Map<String, String> runningContainers = new HashMap<>();
    private final Logger LOGGER = Logger.getLogger(DockerInfoImpl.class.toString());

    private final DockerClient dockerClient;
    private final FrameworkDiscoveryListener frameworkDiscoveryListener;

    public DockerInfoImpl(DockerClient dockerClient, FrameworkDiscoveryListener frameworkDiscoveryListener) {
        this(dockerClient, frameworkDiscoveryListener, 5000);
    }

    public DockerInfoImpl(DockerClient dockerClient, FrameworkDiscoveryListener frameworkDiscoveryListener, long pollInterval) {
        this.dockerClient = dockerClient;
        this.frameworkDiscoveryListener = frameworkDiscoveryListener;
        updateContainerState();
        startPoll(pollInterval);
    }

    public Set<String> getRunningContainers() {
        return this.runningContainers.keySet();
    }

    public String getImageNameOfContainer(String containerId) {
        return this.runningContainers.get(containerId);
    }

    private List<Container> getContainers() throws DockerException, InterruptedException {
        return this.dockerClient.listContainers();
    }

    private List<String> getContainerImageNames() {
        return new ArrayList<>(new HashSet<>(this.runningContainers.values()));
    }

    private void notifyFrameworkListener() {
        LOGGER.info("Notifying about running containers");
        this.frameworkDiscoveryListener.frameworksDiscovered(getContainerImageNames());
    }

    private void startPoll(long pollInterval) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateContainerState();
            }
        }, 0, pollInterval);
    }

    private void updateContainerState() {
        try {
            List<Container> latestRunningContainers = getContainers();

            LOGGER.info(String.format("Found %d running containers", latestRunningContainers.size()));

            Map<String, String> latestRunningContainerIdAndNames = getContainerIdAndNames(latestRunningContainers);

            if (!latestRunningContainerIdAndNames.keySet().equals(this.runningContainers.keySet())) {

                this.runningContainers = latestRunningContainerIdAndNames;

                notifyFrameworkListener();
            }
        } catch (DockerException e) {
            LOGGER.error(String.format("There was an error updating containers: %s", e));
        } catch (InterruptedException e) {
            LOGGER.error(String.format("There was an error updating containers: %s", e));
        }
    }

    private Map<String, String> getContainerIdAndNames(List<Container> containers) {
        Map<String, String> containerIdsAndNames = new HashMap<>();
        for (Container c : containers) {
            containerIdsAndNames.put(c.id(), c.image());
        }
        return containerIdsAndNames;
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
