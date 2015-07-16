package org.apache.mesos.logstash.executor;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogSteamManager;
import org.apache.mesos.logstash.executor.state.DockerInfoCache;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.*;

public class ConfigManagerTest {

    private ConfigManager configManager;
    private ContainerizerClient client;
    private LogstashManager logstash;

    @Before
    public void s() {
        client = mock(ContainerizerClient.class);
        logstash = mock(LogstashManager.class);
        DockerLogSteamManager streamManager = mock(DockerLogSteamManager.class);

        configManager = new ConfigManager(client, logstash, streamManager, new DockerInfoCache());
    }

    @Test
    public void onlyWritesConfigsForRunningContainers() {
        List<FrameworkInfo> frameworks = Lists.newArrayList();
        frameworks.add(new FrameworkInfo("foo", "foo-config"));
        frameworks.add(new FrameworkInfo("bar", "bar-config"));
        frameworks.add(new FrameworkInfo("bas", "bas-config"));

        Set<String> containerIds = Sets.newHashSet("123", "456", "789", "012");

        when(client.getRunningContainers()).thenReturn(containerIds);
        when(client.getImageNameOfContainer("123")).thenReturn("foo");
        when(client.getImageNameOfContainer("456")).thenReturn("quux");
        when(client.getImageNameOfContainer("789")).thenReturn("bas");
        when(client.getImageNameOfContainer("012")).thenReturn("foo");

        configManager.onConfigUpdated(LogType.DOCKER, frameworks.stream());

        verify(logstash, times(1)).updateConfig(LogType.DOCKER, "# bas\nbas-config\n# foo\nfoo-config\n# foo\nfoo-config");
    }

}
