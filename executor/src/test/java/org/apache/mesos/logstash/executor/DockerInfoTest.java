package org.apache.mesos.logstash.executor;


//import com.github.dockerjava.api.DockerClient;
//import com.github.dockerjava.api.command.InspectContainerCmd;
//import com.github.dockerjava.api.command.InspectContainerResponse;
//import com.github.dockerjava.api.command.ListContainersCmd;
//import com.github.dockerjava.api.model.Container;
//import com.github.dockerjava.api.model.ContainerConfig;
//import org.junit.Before;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.*;

/**
* Created by ero on 16/06/15.
*/
public class DockerInfoTest {
//
//    private Container getContainer(final String id) {
//        return new Container() {
//            @Override
//            public String getId() {
//                return id;
//            }
//        };
//    }
//
//    private ContainerConfig getContainerConfig(final String logLocation, final String configFile) {
//        return new ContainerConfig() {
//            @Override
//            public String[] getEnv() {
//                if (logLocation != null && configFile != null) {
//                    String[] envArray = {"LOG_LOCATION=" + logLocation, "CONFIG_FILE=" + configFile};
//                    return envArray;
//                }
//                return new String[] {};
//            }
//        };
//    }
//
//    private InspectContainerResponse getContainerResponse(final String id, final String logLocation, final String configFile) {
//        return new InspectContainerResponse() {
//            @Override
//            public ContainerConfig getConfig() {
//                return getContainerConfig(logLocation, configFile);
//            }
//
//            @Override
//            public String getId() {
//                return id;
//            }
//        };
//    }
//
//    private DockerClient dockerClientStub;
//    private ListContainersCmd listContainersCmd;
//
//    private void mockListCommand(List<Container> containers) {
//        when(listContainersCmd.exec()).thenReturn(containers);
//    }
//
//    private void mockInspectCommand(String containerId, InspectContainerResponse containerResponse) {
//        InspectContainerCmd inspectContainerCmd = mock(InspectContainerCmd.class);
//
//        when(dockerClientStub.inspectContainerCmd(containerId)).thenReturn(inspectContainerCmd);
//        when(inspectContainerCmd.exec()).thenReturn(containerResponse);
//    }
//
//    @Before
//    public void setup() {
//        dockerClientStub = mock(DockerClient.class);
//        listContainersCmd = mock(ListContainersCmd.class);
//        when(dockerClientStub.listContainersCmd()).thenReturn(listContainersCmd);
//    }
//
//    @Test
//    public void testGetContainersThatWantsLoggingExpectingSingleResult() {
//        final String logLocation = "path/to/logs";
//        final String configFile = "path/to/config";
//        final String containerId = "TEST_CONTAINER_ID";
//        final List<Container> containerIds = Collections.singletonList(getContainer(containerId));
//
//        //
//        // Arrange
//        //
//        this.mockListCommand(containerIds);
//        this.mockInspectCommand(containerId, getContainerResponse(containerId, logLocation, configFile));
//
//        //
//        // Act
//        //
//        DockerInfo target = new DockerInfoImpl(dockerClientStub);
//        Map<String, LogstashInfo> result = target.getContainersThatWantLogging();
//
//        //
//        // Assert
//        //
//        assertEquals(result.get(containerId).getLoggingLocationPath(), logLocation);
//        assertEquals(result.get(containerId).getConfiguration(), configFile);
//    }
//
//    @Test
//    public void testGetContainersThatWantsLoggingExpectingOnlyContainerThatSpecifiesLogstashEnvironment() {
//        final String logLocation = "path/to/logs";
//        final String configFile = "path/to/config";
//        final String containerWithLoggingNeeds = "TEST_CONTAINER_ID";
//        final String containerWithoutLoggingNeeds = "TEST_CONTAINER_ID_NO_LOGGING";
//        final List<Container> containerIds = new ArrayList<Container>() {{
//            add(getContainer(containerWithLoggingNeeds));
//            add(getContainer(containerWithoutLoggingNeeds));
//        }};
//
//        //
//        // Arrange
//        //
//        this.mockListCommand(containerIds);
//        this.mockInspectCommand(containerWithLoggingNeeds, getContainerResponse(containerWithLoggingNeeds, logLocation, configFile));
//        this.mockInspectCommand(containerWithoutLoggingNeeds, getContainerResponse(containerWithoutLoggingNeeds, null, null));
//
//        //
//        // Act
//        //
//        DockerInfo target = new DockerInfoImpl(dockerClientStub);
//        Map<String, LogstashInfo> result = target.getContainersThatWantLogging();
//
//        //
//        // Assert
//        //
//        assertTrue(result.containsKey(containerWithLoggingNeeds));
//        assertFalse(result.containsKey(containerWithoutLoggingNeeds));
//    }
}
