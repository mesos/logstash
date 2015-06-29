package org.apache.mesos.logstash.executor;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
* Created by ero on 16/06/15.
*/
public class DockerInfoTest {

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

    private DockerClient dockerClientStub;

    private void mockListCommand(List<Container> firstResult, List<Container>... restResults) {
        try {
            when(dockerClientStub.listContainers()).thenReturn(firstResult, restResults);
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
    public void testGetRunningContainersExpectingTwoContainers() {
        final String container1 = "TEST_CONTAINER_ID1";
        final String container2 = "TEST_CONTAINER_ID2";
        final String imageName1 = "TEST_IMAGE_NAME1";
        final String imageName2 = "TEST_IMAGE_NAME2";
        final List<Container> containerIds = new ArrayList<Container>() {{
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
        DockerInfo target = new DockerInfoImpl(dockerClientStub, mock(FrameworkDiscoveryListener.class));
        Set<String> result = target.getRunningContainers();

        //
        // Assert
        //
        assertTrue(result.contains(container1));
        assertTrue(result.contains(container2));
    }

    @Test
    public void testGetNotifiedAboutCurrentRunningContainers() {
        final String containerId = "TEST_CONTAINER_ID";
        final String imageName = "TEST_IMAGE_NAME";
        final List<Container> containerIds = Collections.singletonList(getContainer(containerId, imageName));

        //
        // Arrange
        //
        this.mockListCommand(containerIds);
        FrameworkDiscoveryListener frameworkDiscoveryListenerSpy = mock(FrameworkDiscoveryListener.class);
        ArgumentCaptor<List> containersCapture = ArgumentCaptor.forClass(List.class);

        //
        // Act
        //
        DockerInfo target = new DockerInfoImpl(dockerClientStub, frameworkDiscoveryListenerSpy);

        //
        // Assert
        //
        verify(frameworkDiscoveryListenerSpy).frameworksDiscovered(containersCapture.capture());
        assertEquals(imageName, containersCapture.getValue().get(0));
    }

    @Test
    public void testGetNotifiedAboutNewRunningContainers() {
        final String container1 = "TEST_CONTAINER_ID1";
        final String container2 = "TEST_CONTAINER_ID2";
        final String imageName1 = "TEST_IMAGE_NAME1";
        final String imageName2 = "TEST_IMAGE_NAME2";
        final List<Container> containerIds = new ArrayList<Container>() {{
            add(getContainer(container1, imageName1));
            add(getContainer(container2, imageName2));
        }};

        //
        // Arrange
        //
        this.mockListCommand(Collections.singletonList(getContainer(container1, imageName1)), containerIds);
        final FrameworkDiscoveryListener frameworkDiscoveryListenerSpy = mock(FrameworkDiscoveryListener.class);
        final ArgumentCaptor<List> containersCapture = ArgumentCaptor.forClass(List.class);

        //
        // Act
        //
        DockerInfo target = new DockerInfoImpl(dockerClientStub, frameworkDiscoveryListenerSpy, 100);

        //
        // Assert
        //
        await().until(new Runnable() {
            @Override
            public void run() {
                verify(frameworkDiscoveryListenerSpy, times(2)).frameworksDiscovered(containersCapture.capture());

                //First call with only one container
                assertEquals(imageName1, containersCapture.getAllValues().get(0).get(0));

                //Second call with both containers
                assertTrue(containersCapture.getAllValues().get(1).contains(imageName1));
                assertTrue(containersCapture.getAllValues().get(1).contains(imageName2));
            }
        });
    }
}
