package org.apache.mesos.logstash.systemtest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.state.Framework;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.*;

/**
 * Tests whether the framework is deployed correctly
 */
public class DeploymentSystemTest {

    public MesosCluster cluster = MesosCluster.builder().numberOfSlaves(1).privateRegistryPort(3333)
            .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
            .build();

    @Before
    public void before() {
        cluster.start();
    }

    @After
    public void after() {
        cluster.stop();
    }

    @Test
    public void testDeployment() throws JsonParseException, UnirestException, JsonMappingException {
        cluster.injectImage("mesos/logstash-executor");

        DockerClientConfig.DockerClientConfigBuilder builder = DockerClientConfig.createDefaultConfigBuilder();

        String dockerHost = System.getenv("DOCKER_HOST");
        if(StringUtils.isBlank(dockerHost)) {
            builder.withUri("unix:///var/run/docker.sock" );
        }

        DockerClientConfig config = builder.build();
        DockerClient dockerClient = DockerClientBuilder.getInstance(config).build();

        String mesosIpAddress = cluster.getMesosContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(dockerClient, mesosIpAddress);
        cluster.addAndStartContainer(container);

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Framework framework = cluster.getStateInfo().getFramework("logstash");
                return
                    framework != null && framework.getTasks().size() > 0 &&
                    framework.getTasks().get(0).getState().equals("TASK_RUNNING");
            }
        });
    }

}
