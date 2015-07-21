package org.apache.mesos.logstash.executor.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import org.apache.mesos.logstash.executor.StartupListener;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.HOURS;

public class DockerClient implements ContainerizerClient, StartupListener {

    private Map<String, String> runningContainers = new HashMap<>();
    private final Logger LOGGER = LoggerFactory.getLogger(DockerClient.class.toString());

    private com.spotify.docker.client.DockerClient dockerClient;
    private Consumer<List<String>> frameworkDiscoveryListener;

    public void setDelegate(Consumer<List<String>> consumer) {
        this.frameworkDiscoveryListener = consumer;
    }

    public DockerClient() {
    }

    public DockerClient(com.spotify.docker.client.DockerClient dockerClient) {
        this.dockerClient = dockerClient;
    }

    public void startMonitoringContainerState() {
        // FIXME: We are never stopping this. Maybe consider making this class a service.

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateContainerState();
            }
        }, 0, (long) 5000);
    }

    public void startupComplete(String hostName) {
        this.dockerClient = DefaultDockerClient.builder()
            .readTimeoutMillis(HOURS.toMillis(1))
            .uri(URI.create("http://" + hostName + ":2376"))
            .build();
    }

    public Set<String> getRunningContainers() {
        return this.runningContainers.keySet();
    }

    public String getImageNameOfContainer(String containerId) {
        return this.runningContainers.get(containerId);
    }

    private List<Container> getContainers() throws DockerException, InterruptedException {
        if (dockerClient == null) {
            return Collections.emptyList();
        }
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

    void updateContainerState() {

        // TODO: Handle state properly.

        try {
            List<Container> latestRunningContainers = getContainers();

            LOGGER
                .info(String.format("Found %d running containers", latestRunningContainers.size()));

            Map<String, String> latestRunningContainerIdAndNames = getContainerIdAndNames(
                latestRunningContainers);

            if (!latestRunningContainerIdAndNames.keySet()
                .equals(this.runningContainers.keySet())) {
                LOGGER.info("Container list changed!");

                this.runningContainers = latestRunningContainerIdAndNames;

                notifyFrameworkListener();
            }
        } catch (DockerException | InterruptedException e) {
            throw new IllegalStateException("Failed to update container states", e);
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
            String id = dockerClient.execCreate(containerId, command,
                com.spotify.docker.client.DockerClient.ExecParameter.STDOUT,
                com.spotify.docker.client.DockerClient.ExecParameter.STDERR);
            return new DockerLogStream(dockerClient.execStart(id));

        } catch (DockerException | InterruptedException e) {
            LOGGER.error(String.format("Error executing in container %s: %s", containerId, e));
        }

        return null;
    }
}
