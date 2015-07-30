package org.apache.mesos.logstash.executor.util;
import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType;
import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
public class ConfigUtilTest {
    private ContainerizerClient client;

    private FrameworkDescription framework1 = new FrameworkDescription("foo", LogstashConfigType.DOCKER);
    private FrameworkDescription framework2 = new FrameworkDescription("bar", LogstashConfigType.DOCKER);
    private FrameworkDescription framework3 = new FrameworkDescription("baz", LogstashConfigType.DOCKER);
    private FrameworkDescription framework4 = new FrameworkDescription("far", LogstashConfigType.HOST);

    @Before
    public void setup() {
        client = mock(ContainerizerClient.class);

        configureMocks(framework1, framework2, framework3, framework4);
    }



    @Test
    public void onlyWritesConfigsForRunningContainers() {
        List<LogstashConfig> frameworks = asFrameworkInfoList(framework1, framework2, framework3);
        List<LogstashConfig> hostFrameworks = asFrameworkInfoList(framework4);

        configureAsRunningFrameworks(framework1, framework2, framework3);

        String generateConfigFile = ConfigUtil.generateConfigFile(client, frameworks, hostFrameworks);


        Assert.assertTrue(generateConfigFile.contains(String.format("# %s\n%s\n",framework1.frameworkInfo.getFrameworkName(), framework1.frameworkInfo.getConfig())));
        Assert.assertTrue(generateConfigFile.contains(String.format("# %s\n%s\n",framework2.frameworkInfo.getFrameworkName(), framework2.frameworkInfo.getConfig())));
        Assert.assertTrue(generateConfigFile.contains(String.format("# %s\n%s\n",framework3.frameworkInfo.getFrameworkName(), framework3.frameworkInfo.getConfig())));
    }



    private void configureAsRunningFrameworks(FrameworkDescription... frameworkDescriptions){
        for (FrameworkDescription desc : frameworkDescriptions){
            when(client.getRunningContainers()).thenReturn(asContainerIdSet(frameworkDescriptions));
        }
    }

    private void configureMocks(FrameworkDescription ... frameworkDescriptions){
        for (FrameworkDescription desc : frameworkDescriptions){
            when(client.getImageNameOfContainer(desc.id)).thenReturn(desc.frameworkInfo.getFrameworkName());
        }
    }

    private Set<String> asContainerIdSet(FrameworkDescription ... frameworkDescriptions){
        return Arrays.asList(frameworkDescriptions).stream().map(FrameworkDescription::getId).collect(
            Collectors.toSet());
    }

    private List<LogstashConfig> asFrameworkInfoList(
        FrameworkDescription... frameworkDescriptions){
        return Arrays.asList(frameworkDescriptions).stream().map(FrameworkDescription::getFrameworkInfo).collect(
            Collectors.toList());
    }


    private static class FrameworkDescription {
        final LogstashConfig frameworkInfo;
        final String id;

        public String getId() {
            return id;
        }

        private FrameworkDescription(String id, LogstashConfigType type) {
            this.frameworkInfo = LogstashConfig.newBuilder().setType(type).setFrameworkName(id + "-name").setConfig(id + "-config").build();
            this.id = id;
        }

        public LogstashConfig getFrameworkInfo() {
            return frameworkInfo;
        }
    }

}