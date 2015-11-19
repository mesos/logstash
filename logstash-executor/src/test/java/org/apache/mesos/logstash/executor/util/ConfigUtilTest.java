package org.apache.mesos.logstash.executor.util;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType;
import org.apache.mesos.logstash.executor.docker.DockerClient;
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
    private DockerClient client;

    private FrameworkDescription framework1 = new FrameworkDescription("foo", LogstashConfigType.DOCKER);
    private FrameworkDescription framework2 = new FrameworkDescription("bar", LogstashConfigType.DOCKER);
    private FrameworkDescription framework3 = new FrameworkDescription("baz", LogstashConfigType.DOCKER);
    private FrameworkDescription framework4 = new FrameworkDescription("far", LogstashConfigType.HOST);

    @Before
    public void setup() {
        client = mock(DockerClient.class);

        configureMocks(framework1, framework2, framework3, framework4);
    }

    private void configureMocks(FrameworkDescription ... frameworkDescriptions){
        for (FrameworkDescription desc : frameworkDescriptions){
            when(client.getImageNameOfContainer(desc.id)).thenReturn(desc.frameworkInfo.getFrameworkName());
        }
    }

    private static class FrameworkDescription {
        final LogstashConfig frameworkInfo;
        final String id;

        private FrameworkDescription(String id, LogstashConfigType type) {
            this.frameworkInfo = LogstashConfig.newBuilder().setType(type).setFrameworkName(id + "-name").setConfig(id + "-config").build();
            this.id = id;
        }
    }

}