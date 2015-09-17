package org.apache.mesos.logstash.executor.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import org.apache.mesos.logstash.common.ConcurrentUtils;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.HOURS;

/**
 * Docker client wrapper.
 */
public class DockerClient {

    private Map<String, String> runningContainers = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerClient.class);

    private com.spotify.docker.client.DockerClient dockerClient;
    private Consumer<List<String>> frameworkDiscoveryListener;

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    public void setDelegate(Consumer<List<String>> consumer) {
        this.frameworkDiscoveryListener = consumer;
    }

    public DockerClient() {

    }

    public DockerClient(com.spotify.docker.client.DockerClient dockerClient) {
        this.dockerClient = dockerClient;
    }

    public void start() {
        executorService.scheduleWithFixedDelay(this::updateContainerState, 0, 5, TimeUnit.SECONDS);
    }

    public void stop() {
        ConcurrentUtils.stop(executorService);
    }

    public void startupComplete(String hostName) {
        // There is currently no what (what we know of) to reliably
        // get the address of the host we are running on without waiting
        // for the executor to register itself. This is called by the
        // executor itself.

        this.dockerClient = DefaultDockerClient.builder()
            .readTimeoutMillis(HOURS.toMillis(1))
            .uri(URI.create("http://" + hostName + ":2376"))
            .build();
    }

    public Set<String> getRunningContainers() {
        return this.runningContainers.keySet();
    }

    // TODO (Florian) this method does not just return an image name
    // only if we tracked the container as running too -> refactor
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

            LOGGER.info(
                String.format("Found %d running containers", latestRunningContainers.size()));

            Map<String, String> latestRunningContainerIdAndNames = getContainerIdAndNames(
                latestRunningContainers);

            if (!latestRunningContainerIdAndNames.keySet()
                .equals(this.runningContainers.keySet())) {
                LOGGER.info("Container list changed!");

                this.runningContainers = latestRunningContainerIdAndNames;

                notifyFrameworkListener();
            }
        } catch (DockerException | InterruptedException e) {
            throw new IllegalStateException("Failed to onNewConfigsFromScheduler container states", e);
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
                com.spotify.docker.client.DockerClient.ExecParameter.STDOUT);

            return new DockerLogStream(dockerClient.execStart(id));

        } catch (DockerException | InterruptedException e) {
            LOGGER.error(String.format("Error executing in container %s: %s", containerId, e));
        }

        return null;
    }
}
