package org.apache.mesos.logstash.executor.docker;
import org.apache.mesos.logstash.executor.TestableLogStream;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
public class DockerLogSteamManagerTest {

    public static final String SOME_CONTAINER_ID_1 = "someContainerId";
    public static final String SOME_CONTAINER_ID_2 = "someOtherContainerId";
    public static final String SOME_LOG_FILE_1 = "/var/log/foo.log";
    public static final String SOME_LOG_FILE_2 = "/var/log/bar.log";
    public static final String SOME_LOG_FILE_3 = "/var/log/baz.log";
    public static final String SOME_FRAMEWORK_NAME = "some framework name";
    private DockerStreamer streamer;


    ArgumentCaptor<DockerLogPath> dockerLogPathArgumentCaptor = ArgumentCaptor.forClass(DockerLogPath.class);
    ArgumentCaptor<LogStream> logStreamArgumentCaptor = ArgumentCaptor.forClass(LogStream.class);


    private  DockerLogSteamManager dockerLogStreamManager;

    @Before
    public void setup(){
        streamer = mock(DockerStreamer.class);

        dockerLogStreamManager = new DockerLogSteamManager(streamer);

        // return a new instance of a LogStream for each start streaming call
        when(streamer.startStreaming(any())).thenAnswer(new Answer<LogStream>() {
            @Override public LogStream answer(InvocationOnMock invocationOnMock) throws Throwable {
                return mock(TestableLogStream.class);
            }
        });
    }


    @Test
    public void setupContainerLogfileStreaming_withSeveralNewLogFiles_shouldStreamAllOfThem() throws Exception {
        DockerFramework framework = createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2);

        dockerLogStreamManager.setupContainerLogfileStreaming(framework);

        verify(streamer, times(2)).startStreaming(dockerLogPathArgumentCaptor.capture());
        assertEquals(
            new DockerLogPath(SOME_CONTAINER_ID_1, SOME_FRAMEWORK_NAME, SOME_LOG_FILE_1),
            dockerLogPathArgumentCaptor.getAllValues().get(0));

        assertEquals(
            new DockerLogPath(SOME_CONTAINER_ID_1, SOME_FRAMEWORK_NAME, SOME_LOG_FILE_2),
            dockerLogPathArgumentCaptor.getAllValues().get(1));
    }

    @Test
    public void setupContainerLogfileStreaming_withSeveralNewLogFilesInNextUpdate_processOnlyNewFiles() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1));

        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2));

        verify(streamer, times(2)).startStreaming(dockerLogPathArgumentCaptor.capture());
        assertEquals(
            new DockerLogPath(SOME_CONTAINER_ID_1, SOME_FRAMEWORK_NAME, SOME_LOG_FILE_1),
            dockerLogPathArgumentCaptor.getAllValues().get(0));
        assertEquals(
            new DockerLogPath(SOME_CONTAINER_ID_1, SOME_FRAMEWORK_NAME, SOME_LOG_FILE_2),
            dockerLogPathArgumentCaptor.getAllValues().get(1));
    }

    @Test
    public void setupContainerLogfileStreaming_withSomeAlreadyStreamedLogFilesAreNotIncludedNextUpdate_stopProcessingOfTheseStreams() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2));
        LogStream logStreamOfFile2 = getLogStream(SOME_CONTAINER_ID_1, SOME_LOG_FILE_2);

        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1));

        verify(streamer, times(1)).stopStreaming(logStreamArgumentCaptor.capture());
        assertEquals(
            logStreamOfFile2,
            logStreamArgumentCaptor.getValue());
    }


    @Test
    public void stopStreamingForWholeFramework_withSomeAlreadyStreamedLogFiles_stopProcessingOfTheseStreamsAndRemoveTracking() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2));
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_2, SOME_LOG_FILE_1, SOME_LOG_FILE_2));
        LogStream logStreamOfFile1 = getLogStream(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1);
        LogStream logStreamOfFile2 = getLogStream(SOME_CONTAINER_ID_1, SOME_LOG_FILE_2);

        dockerLogStreamManager.stopStreamingForWholeFramework(SOME_CONTAINER_ID_1);


        verify(streamer, times(2)).stopStreaming(logStreamArgumentCaptor.capture());

        assertTrue(logStreamArgumentCaptor.getAllValues().contains(logStreamOfFile1));
        assertTrue(logStreamArgumentCaptor.getAllValues().contains(logStreamOfFile2));

        assertFalse(dockerLogStreamManager.getProcessedContainers().contains(SOME_CONTAINER_ID_1));
        assertTrue(dockerLogStreamManager.getProcessedContainers().contains(SOME_CONTAINER_ID_2)); // this container was not stopped
    }


    @Test
    public void getProcessedContainers_withSeveralLogfilesForOneFramework() throws Exception {
        DockerFramework framework = createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2);

        dockerLogStreamManager.setupContainerLogfileStreaming(framework);


        assertEquals(1, dockerLogStreamManager.getProcessedContainers().size());
        assertEquals(SOME_CONTAINER_ID_1,
            dockerLogStreamManager.getProcessedContainers().iterator().next());
    }

    @Test
    public void getProcessedContainers_withSeveralLogfilesForSeveralFrameworks() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2));
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_2, SOME_LOG_FILE_1, SOME_LOG_FILE_2));


        assertEquals(2, dockerLogStreamManager.getProcessedContainers().size());
        assertTrue(dockerLogStreamManager.getProcessedContainers().contains(SOME_CONTAINER_ID_1));
        assertTrue(dockerLogStreamManager.getProcessedContainers().contains(SOME_CONTAINER_ID_2));
    }

    @Test
    public void getProcessedContainers_withSeveralLogfilesForSeveralFrameworks_processOnlyNewFrameworks() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1));

        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_2)); // this container is allready processed

        assertEquals(1, dockerLogStreamManager.getProcessedContainers().size());
        assertTrue(dockerLogStreamManager.getProcessedContainers().contains(SOME_CONTAINER_ID_1));
    }

    @Test
    public void getProcessedFiles_withContainerIsProcessed_returnsEmptyList() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1, SOME_LOG_FILE_2));

        assertEquals(0, dockerLogStreamManager.getProcessedFiles("UNKNOWN").size());
    }

    @Test
    public void getProcessedFiles_withSeveralFilesForRequestedContianerId_shouldNotContainerDuplicates() throws Exception {
        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1));

        dockerLogStreamManager.setupContainerLogfileStreaming(
            createDockerFramework(SOME_CONTAINER_ID_1, SOME_LOG_FILE_1,SOME_LOG_FILE_2));

        assertEquals(2, dockerLogStreamManager.getProcessedFiles(SOME_CONTAINER_ID_1).size());

        Set<String> logPaths=  dockerLogStreamManager.getProcessedFiles(SOME_CONTAINER_ID_1).stream()
            .map(DockerLogPath::getContainerLogPath).collect(Collectors.toSet());

        assertTrue(logPaths.contains(SOME_LOG_FILE_1));
        assertTrue(logPaths.contains(SOME_LOG_FILE_2));
    }

    private DockerFramework createDockerFramework(String containerId, String ... logfiles) {
        String logLocations  = Stream.of(logfiles).map(f -> String.format("docker-path => \"%s\"", f)).collect(
            Collectors.joining("\n"));

        return new DockerFramework(new FrameworkInfo(SOME_FRAMEWORK_NAME, logLocations), new DockerFramework.ContainerId(
                containerId));
    }


    private LogStream getLogStream(String containerId, final String logFile) {
        return dockerLogStreamManager.processedContainers.get(containerId).stream()
            .filter(
                new Predicate<DockerLogSteamManager.ProcessedDockerLogPath>() {
                    @Override public boolean test(
                        DockerLogSteamManager.ProcessedDockerLogPath processedDockerLogPath) {
                        return processedDockerLogPath.dockerLogPath.getContainerLogPath()
                            .equals(logFile);
                    }
                }).collect(Collectors.toSet()).iterator().next().logStream;
    }
}