package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogStreamManager;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ConfigManagerTest {

    private ConfigManager configManager;
    private ContainerizerClient client;
    private LogstashService logstash;
    private DockerLogStreamManager streamManager;

    ArgumentCaptor<DockerFramework> dockerFrameworkArgumentCaptor = ArgumentCaptor
        .forClass(DockerFramework.class);
    ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);

    private FrameworkDescription framework1 = new FrameworkDescription("foo");
    private FrameworkDescription framework2 = new FrameworkDescription("bar");
    private FrameworkDescription framework3 = new FrameworkDescription("baz");
    private FrameworkDescription framework4 = new FrameworkDescription("far");

    @Before
    public void setup() {
        client = mock(ContainerizerClient.class);
        logstash = mock(LogstashService.class);
        streamManager = mock(DockerLogStreamManager.class);

        configManager = new ConfigManager(logstash, client, streamManager);

        configureMocks(framework1, framework2, framework3, framework4);
    }

    @Test
    public void onNewConfigsFromScheduler_callsLogstashServiceWithFrameworkInfos() {
        List<FrameworkInfo> dockerFrameworks = asFrameworkInfoList(framework1, framework2,
            framework3);
        List<FrameworkInfo> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1, framework2, framework3);

        configManager.onNewConfigsFromScheduler(hostFrameworks, dockerFrameworks);

        verify(logstash, times(1)).update(dockerFrameworks, hostFrameworks);
    }

    @Test
    public void onNewConfigsFromScheduler_shouldStartContainerLogfileFileStreamingWithCorrectDockerFramework() {
        List<FrameworkInfo> dockerFrameworks = asFrameworkInfoList(framework1, framework2,
            framework3);
        List<FrameworkInfo> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1);

        configManager.onNewConfigsFromScheduler(hostFrameworks, dockerFrameworks);

        verify(streamManager, times(1)).setupContainerLogfileStreaming(
            dockerFrameworkArgumentCaptor.capture());

        DockerFramework expectedDockerFramework = new DockerFramework(framework1.getFrameworkInfo(),
            new DockerFramework.ContainerId(framework1.getId()));

        assertEquals(expectedDockerFramework.getContainerId(),
            dockerFrameworkArgumentCaptor.getAllValues().get(0).getContainerId());
        assertEquals(expectedDockerFramework.getConfiguration(),
            dockerFrameworkArgumentCaptor.getAllValues().get(0).getConfiguration());
        assertEquals(expectedDockerFramework.getName(),
            dockerFrameworkArgumentCaptor.getAllValues().get(0).getName());
    }

    @Test
    public void onNewConfigsFromScheduler_shouldStartContainerLogFileStreamingWithAllMatchingContainers() {
        List<FrameworkInfo> dockerFrameworks = asFrameworkInfoList(framework1, framework2,
            framework3);
        List<FrameworkInfo> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1, framework3);

        configManager.onNewConfigsFromScheduler(hostFrameworks, dockerFrameworks);

        verify(streamManager, times(2)).setupContainerLogfileStreaming(
            dockerFrameworkArgumentCaptor.capture());

        assertTrue(asContainerIdSet(framework1, framework3).contains(
            dockerFrameworkArgumentCaptor.getAllValues().get(0).getContainerId()));

        assertTrue(asContainerIdSet(framework1, framework3).contains(
            dockerFrameworkArgumentCaptor.getAllValues().get(1).getContainerId()));
    }

    @Test
    public void onNewConfigsFromScheduler_shouldStopRunningContainerLogFileStreamingWithNoMatchingConfiguration() {
        List<FrameworkInfo> dockerFrameworks = asFrameworkInfoList(
            framework1); // only one docker container config, but three running container which were streaming
        List<FrameworkInfo> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1, framework2, framework3);

        configManager.onNewConfigsFromScheduler(hostFrameworks, dockerFrameworks);

        verify(streamManager, times(2)).stopStreamingForWholeFramework(
            stringArgumentCaptor.capture());

        assertTrue(asContainerIdSet(framework2, framework3).contains(
            stringArgumentCaptor.getAllValues().get(0)));

        assertTrue(asContainerIdSet(framework2, framework3).contains(
            stringArgumentCaptor.getAllValues().get(1)));
    }

    @Test
    public void onNewConfigsFromScheduler_shouldNotStopAnyRunningContainerLogFileStreamingWhenAllHaveMatchingConfiguration() {
        List<FrameworkInfo> dockerFrameworks = asFrameworkInfoList(
            framework1, framework2, framework3);
        List<FrameworkInfo> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1, framework2, framework3);

        configManager.onNewConfigsFromScheduler(hostFrameworks, dockerFrameworks);

        verify(streamManager, times(0)).stopStreamingForWholeFramework(
            stringArgumentCaptor.capture());
    }

    private void configureAsRunningFrameworks(FrameworkDescription... frameworkDescriptions) {
        when(client.getRunningContainers()).thenReturn(asContainerIdSet(frameworkDescriptions));
        when(streamManager.getProcessedContainers())
            .thenReturn(asContainerIdSet(frameworkDescriptions));
    }

    private void configureMocks(FrameworkDescription... frameworkDescriptions) {
        for (FrameworkDescription desc : frameworkDescriptions) {
            when(client.getImageNameOfContainer(desc.id)).thenReturn(desc.frameworkInfo.getName());
        }
    }

    private Set<String> asContainerIdSet(FrameworkDescription... frameworkDescriptions) {
        return Arrays.asList(frameworkDescriptions).stream().map(FrameworkDescription::getId)
            .collect(
                Collectors.toSet());
    }

    private List<FrameworkInfo> asFrameworkInfoList(FrameworkDescription... frameworkDescriptions) {
        return Arrays.asList(frameworkDescriptions).stream()
            .map(FrameworkDescription::getFrameworkInfo).collect(
                Collectors.toList());
    }

    private static class FrameworkDescription {
        final FrameworkInfo frameworkInfo;
        final String id;

        public String getId() {
            return id;
        }

        private FrameworkDescription(String id) {
            this.frameworkInfo = new FrameworkInfo(id + "-ID", id + "-config");
            this.id = id;
        }

        public FrameworkInfo getFrameworkInfo() {
            return frameworkInfo;
        }
    }

}
