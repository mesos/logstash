package org.apache.mesos.logstash.executor.docker;

import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import org.apache.log4j.Logger;
import org.apache.mesos.logstash.executor.logging.LogStream;

import java.util.*;
import java.util.function.Consumer;


public class DockerClient implements ContainerizerClient {

    private Map<String, String> runningContainers = new HashMap<>();
    private final Logger LOGGER = Logger.getLogger(DockerClient.class.toString());

    private final com.spotify.docker.client.DockerClient dockerClient;
    private Consumer<List<String>> frameworkDiscoveryListener;

    public void setDelegate(Consumer<List<String>> consumer) {
        this.frameworkDiscoveryListener = consumer;
    }

    public DockerClient(com.spotify.docker.client.DockerClient dockerClient) {
        this.dockerClient = dockerClient;

        updateContainerState();
        startPoll(5000);
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
        List<String> imageNames = getContainerImageNames();

        for (String name : imageNames) {
            LOGGER.info("Found container running image: " + name);
        }

        LOGGER.info("Notifying about running containers");
        if (this.frameworkDiscoveryListener != null) {
            this.frameworkDiscoveryListener.accept(imageNames);
        } else {
            LOGGER.warn("No listener set!");
        }
    }

    private void startPoll(long pollInterval) {
        // FIXME: We are never stopping this. Maybe consider making this class a service.

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateContainerState();
            }
        }, 0, pollInterval);
    }

    private void updateContainerState() {

        // TODO: Handle state properly.

        try {
            List<Container> latestRunningContainers = getContainers();

            LOGGER.info(String.format("Found %d running containers", latestRunningContainers.size()));

            Map<String, String> latestRunningContainerIdAndNames = getContainerIdAndNames(latestRunningContainers);

            if (!latestRunningContainerIdAndNames.keySet().equals(this.runningContainers.keySet())) {

                this.runningContainers = latestRunningContainerIdAndNames;

                notifyFrameworkListener();
            }
        } catch (DockerException | InterruptedException e) {
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

    public LogStream exec(String containerId, String... command) {

        // TODO: Handle error properly.

        try {
            String id = dockerClient.execCreate(containerId, command, com.spotify.docker.client.DockerClient.ExecParameter.STDOUT, com.spotify.docker.client.DockerClient.ExecParameter.STDERR);
            return new DockerLogStream(dockerClient.execStart(id));

        } catch (DockerException | InterruptedException e) {
            LOGGER.error(String.format("Error executing in container %s: %s", containerId, e));
        }

        return null;
    }
}
