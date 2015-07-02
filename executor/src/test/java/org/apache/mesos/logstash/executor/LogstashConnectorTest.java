package org.apache.mesos.logstash.executor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Created by ero on 01/07/15.
 */
public class LogstashConnectorTest {

    DockerInfo dockerInfoStub;
    LogstashService logstashServiceMock;
    LogfileStreaming logfileStreamingMock;
    LogstashConnector target;

    @Before
    public void setUp() {
        dockerInfoStub = mock(DockerInfo.class);
        logstashServiceMock = mock(LogstashService.class);
        logfileStreamingMock = mock(LogfileStreaming.class);
    }

    @Test
    public void testUpdatedLogLocationsExpectingLocalLogLocationToBeSet() {
        final String containerId = "TEST_CONTAINER_ID";
        final String imageName = "TEST_IMAGE_NAME";
        final String configuration = "{input => {}}";
        List<Framework> frameworks = Collections.singletonList(new DockerFramework(imageName, configuration));

        when(dockerInfoStub.getRunningContainers()).thenReturn(Collections.singleton(containerId));
        when(dockerInfoStub.getImageNameOfContainer(containerId)).thenReturn(imageName);
        when(logfileStreamingMock.isConfigured(containerId)).thenReturn(false);
        when(logstashServiceMock.hasStarted()).thenReturn(false);
        ArgumentCaptor<Map> argumentCaptor = ArgumentCaptor.forClass(Map.class);

        target = new LogstashConnector(dockerInfoStub, logstashServiceMock, logfileStreamingMock);
        target.updatedLogLocations(frameworks);

        verify(logfileStreamingMock).setupContainerLogfileStreaming(containerId, frameworks.get(0));
        verify(logstashServiceMock).reconfigure(argumentCaptor.capture());
        verify(logstashServiceMock).start();

        assertTrue(argumentCaptor.getValue().containsKey(containerId));
        assertTrue(argumentCaptor.getValue().containsValue(frameworks.get(0)));
    }
}
