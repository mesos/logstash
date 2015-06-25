package org.apache.mesos.logstash.executor;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
* Created by ero on 16/06/15.
*/
public class DockerInfoTest {

    private Container getContainer(final String id) {
        return new Container() {
            @Override
            public String id() {
                return id;
            }
        };
    }

    private ContainerConfig getContainerConfig(final String logLocation, final String configFile) {
        List<String> env = new ArrayList<>();
        if (logLocation != null && configFile != null) {
            env = new ArrayList<String>() {{
                add("LOG_LOCATION=" + logLocation);
                add("CONFIG_FILE=" + configFile);
            }};
        }

        return ContainerConfig.builder()
                .env(env)
                .build();
    }

    private ContainerInfo getContainerResponse(final String id, final String logLocation, final String configFile) {
        return new ContainerInfo() {
            @Override
            public ContainerConfig config() {
                return getContainerConfig(logLocation, configFile);
            }

            @Override
            public String id() {
                return id;
            }
        };
    }

    private DockerClient dockerClientStub;

    private void mockListCommand(List<Container> containers) {
        try {
            when(dockerClientStub.listContainers()).thenReturn(containers);
        } catch (DockerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void mockInspectCommand(String containerId, ContainerInfo containerResponse) {
        try {
            when(dockerClientStub.inspectContainer(containerId)).thenReturn(containerResponse);
        } catch (DockerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() {
        dockerClientStub = mock(DockerClient.class);
    }

    @Test
    public void testGetContainersThatWantsLoggingExpectingSingleResult() {
        final String logLocation = "path/to/logs";
        final String configFile = "path/to/config";
        final String containerId = "TEST_CONTAINER_ID";
        final List<Container> containerIds = Collections.singletonList(getContainer(containerId));

        //
        // Arrange
        //
        this.mockListCommand(containerIds);
        this.mockInspectCommand(containerId, getContainerResponse(containerId, logLocation, configFile));

        //
        // Act
        //
        DockerInfo target = new DockerInfoImpl(dockerClientStub);
        Map<String, LogstashInfo> result = target.getContainersThatWantLogging();

        //
        // Assert
        //
        assertEquals(result.get(containerId).getLoggingLocationPath(), logLocation);
        assertEquals(result.get(containerId).getConfiguration(), configFile);
    }

    @Test
    public void testGetContainersThatWantsLoggingExpectingOnlyContainerThatSpecifiesLogstashEnvironment() {
        final String logLocation = "path/to/logs";
        final String configFile = "path/to/config";
        final String containerWithLoggingNeeds = "TEST_CONTAINER_ID";
        final String containerWithoutLoggingNeeds = "TEST_CONTAINER_ID_NO_LOGGING";
        final List<Container> containerIds = new ArrayList<Container>() {{
            add(getContainer(containerWithLoggingNeeds));
            add(getContainer(containerWithoutLoggingNeeds));
        }};

        //
        // Arrange
        //
        this.mockListCommand(containerIds);
        this.mockInspectCommand(containerWithLoggingNeeds, getContainerResponse(containerWithLoggingNeeds, logLocation, configFile));
        this.mockInspectCommand(containerWithoutLoggingNeeds, getContainerResponse(containerWithoutLoggingNeeds, null, null));

        //
        // Act
        //
        DockerInfo target = new DockerInfoImpl(dockerClientStub);
        Map<String, LogstashInfo> result = target.getContainersThatWantLogging();

        //
        // Assert
        //
        assertTrue(result.containsKey(containerWithLoggingNeeds));
        assertFalse(result.containsKey(containerWithoutLoggingNeeds));
    }
}
