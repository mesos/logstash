package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.concurrent.Callable;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests REST node discovery
 */
public class DiscoverySystemTest {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    @Test
    public void testThatOneContainerIsRunning() throws InterruptedException {

        DockerClient outerDockerClient = createDockerClient("unix:///var/run/docker.sock");
        InspectContainerResponse response = outerDockerClient.inspectContainerCmd("mesosls_slave1_1").exec();

        String ipAddress = response.getNetworkSettings().getIpAddress();

        DockerClient innerDockerClient = createDockerClient(String.format("http://%s:2376", ipAddress));

        LOGGER.info(String.format("Asking for containers at %s", ipAddress));

        int numberOfContainersRunning = await().until(containersSize(innerDockerClient.listContainersCmd()), equalTo(1));

        assertEquals(1, numberOfContainersRunning);
    }

    public void testThatAContainerStarts() throws InterruptedException {

    }

    private static DockerClient createDockerClient(String hostAddress) {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withVersion("1.18")
                .withUri(hostAddress)
                .build();

        return DockerClientBuilder.getInstance(config).build();
    }

    private Callable<Integer> containersSize(final ListContainersCmd listContainersCmd) {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return listContainersCmd.exec().size();
            }
        };
    }

}