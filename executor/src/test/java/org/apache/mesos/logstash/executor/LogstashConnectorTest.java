package org.apache.mesos.logstash.executor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
        List<Framework> frameworks = new ArrayList<Framework>() {{
            add(new Framework(imageName, new ArrayList<LogConfiguration>() {{
                add(new LogConfiguration("TYPE", "LOCATION.log", "TAG"));
            }}));
        }};

        when(dockerInfoStub.getRunningContainers()).thenReturn(new HashSet<String>() {{
            add(containerId);
        }});

        when(dockerInfoStub.getImageNameOfContainer(containerId)).thenReturn(imageName);

        target = new LogstashConnector(dockerInfoStub, logstashServiceMock, logfileStreamingMock);
        target.updatedLogLocations(frameworks);

        assertEquals("/tmp/TEST_CONTAINER_ID/TEST_IMAGE_NAME/LOCATION.log", frameworks.get(0).getLogConfigurationList().get(0).getLocalLogLocation());
    }
}
