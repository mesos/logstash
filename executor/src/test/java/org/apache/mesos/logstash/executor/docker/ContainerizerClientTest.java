package org.apache.mesos.logstash.executor.docker;

import com.google.common.collect.Lists;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ContainerizerClientTest {

    private Container getContainer(final String id, final String imageName) {
        return new Container() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public String image() {
                return imageName;
            }
        };
    }

    private com.spotify.docker.client.DockerClient dockerClientStub;

    private void mockListCommand(List<Container> firstResult, List<Container>... restResults) {
        try {
            when(dockerClientStub.listContainers()).thenReturn(firstResult, restResults);
        } catch (DockerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setup() {
        dockerClientStub = mock(com.spotify.docker.client.DockerClient.class);
    }

    @Test
    public void testGetRunningContainersExpectingTwoContainers() {
        String container1 = "TEST_CONTAINER_ID1";
        String container2 = "TEST_CONTAINER_ID2";
        String imageName1 = "TEST_IMAGE_NAME1";
        String imageName2 = "TEST_IMAGE_NAME2";
        List<Container> containerIds = new ArrayList<Container>() {{
            add(getContainer(container1, imageName1));
            add(getContainer(container2, imageName2));
        }};

        //
        // Arrange
        //
        this.mockListCommand(containerIds);

        //
        // Act
        //
        DockerClient target = new DockerClient(dockerClientStub);
        target.updateContainerState();
        Set<String> result = target.getRunningContainers();

        //
        // Assert
        //
        assertTrue(result.contains(container1));
        assertTrue(result.contains(container2));
    }

    @Test
    public void testGetNotifiedAboutCurrentRunningContainers() {
        String containerId = "TEST_CONTAINER_ID";
        String imageName = "TEST_IMAGE_NAME";
        List<Container> containerIds = Collections
            .singletonList(getContainer(containerId, imageName));

        //
        // Arrange
        //
        this.mockListCommand(containerIds);
        ArgumentCaptor<List> containersCapture = ArgumentCaptor.forClass(List.class);
        Consumer<List<String>> consumerMock = mock(Consumer.class);

        //
        // Act
        //
        DockerClient target = new DockerClient(dockerClientStub);
        target.setDelegate(consumerMock);
        target.updateContainerState();

        //
        // Assert
        //

        verify(consumerMock).accept(containersCapture.capture());
        // Second call with both containers
        assertEquals(imageName, containersCapture.getValue().get(0));
    }

    @Test
    public void testGetNotifiedAboutNewRunningContainers() {

        String container1 = "TEST_CONTAINER_ID1";
        String container2 = "TEST_CONTAINER_ID2";
        String imageName1 = "TEST_IMAGE_NAME1";
        String imageName2 = "TEST_IMAGE_NAME2";

        List<Container> firstResponse = Collections
            .singletonList(getContainer(container1, imageName1));
        // at second call it will return a new container id (container2)
        List<Container> secondResponse = Lists.newArrayList(getContainer(container1, imageName1),
            getContainer(container2, imageName2));

        Consumer<List<String>> consumerMock = mock(Consumer.class);
        this.mockListCommand(firstResponse, secondResponse);
        ArgumentCaptor<List> containersCapture = ArgumentCaptor.forClass(List.class);

        DockerClient target = new DockerClient(dockerClientStub);
        target.setDelegate(consumerMock);

        target.updateContainerState();
        target.updateContainerState();

        verify(consumerMock, times(2)).accept(containersCapture.capture());
        assertEquals(imageName1, containersCapture.getAllValues().get(0).get(0));

        //Second call with both containers
        assertTrue(containersCapture.getAllValues().get(1).contains(imageName1));
        assertTrue(containersCapture.getAllValues().get(1).contains(imageName2));
    }
}
