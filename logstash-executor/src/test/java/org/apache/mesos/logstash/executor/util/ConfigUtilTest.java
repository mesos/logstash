package org.apache.mesos.logstash.executor.util;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.mesos.logstash.executor.ConfigManager;
import org.apache.mesos.logstash.executor.LogstashService;
import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogSteamManager;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
public class ConfigUtilTest {
    private ContainerizerClient client;

    private FrameworkDescription framework1 = new FrameworkDescription("foo");
    private FrameworkDescription framework2 = new FrameworkDescription("bar");
    private FrameworkDescription framework3 = new FrameworkDescription("baz");
    private FrameworkDescription framework4 = new FrameworkDescription("far");

    @Before
    public void setup() {
        client = mock(ContainerizerClient.class);

        configureMocks(framework1, framework2, framework3, framework4);
    }



    @Test
    public void onlyWritesConfigsForRunningContainers() {
        List<FrameworkInfo> frameworks = asFrameworkInfoList(framework1, framework2, framework3);
        List<FrameworkInfo> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1, framework2, framework3);

        String generateConfigFile = ConfigUtil.generateConfigFile(client, frameworks, hostFrameworks);


        Assert.assertTrue(generateConfigFile.contains(String.format("# %s\n%s\n",framework1.frameworkInfo.getName(), framework1.frameworkInfo.getConfiguration())));
        Assert.assertTrue(generateConfigFile.contains(String.format("# %s\n%s\n",framework2.frameworkInfo.getName(), framework2.frameworkInfo.getConfiguration())));
        Assert.assertTrue(generateConfigFile.contains(String.format("# %s\n%s\n",framework3.frameworkInfo.getName(), framework3.frameworkInfo.getConfiguration())));
    }



    private void configureAsRunningFrameworks(FrameworkDescription... frameworkDescriptions){
        for (FrameworkDescription desc : frameworkDescriptions){
            when(client.getRunningContainers()).thenReturn(asContainerIdSet(frameworkDescriptions));
        }
    }

    private void configureMocks(FrameworkDescription ... frameworkDescriptions){
        for (FrameworkDescription desc : frameworkDescriptions){
            when(client.getImageNameOfContainer(desc.id)).thenReturn(desc.frameworkInfo.getName());
        }
    }

    private Set<String> asContainerIdSet(FrameworkDescription ... frameworkDescriptions){
        return Arrays.asList(frameworkDescriptions).stream().map(FrameworkDescription::getId).collect(
            Collectors.toSet());
    }

    private List<FrameworkInfo> asFrameworkInfoList(FrameworkDescription ... frameworkDescriptions){
        return Arrays.asList(frameworkDescriptions).stream().map(FrameworkDescription::getFrameworkInfo).collect(
            Collectors.toList());
    }


    private static class FrameworkDescription {
        final FrameworkInfo frameworkInfo;
        final String id;

        public String getId() {
            return id;
        }

        private FrameworkDescription(String id) {
            this.frameworkInfo = new FrameworkInfo(id + "-name", id + "-config");
            this.id = id;
        }

        public FrameworkInfo getFrameworkInfo() {
            return frameworkInfo;
        }
    }

}