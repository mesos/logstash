package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.state.Framework;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.dockerjava.api.DockerClient;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.*;

/**
 * Tests whether the framework is deployed correctly
 */
public class DeploymentSystemTest {

    private static DockerClient dockerClient = DockerClientFactory.build();

    private static MesosCluster cluster = new MesosCluster(new ClusterArchitecture.Builder()
            .withZooKeeper()
            .withMaster()
            .withSlave(zooKeeper -> new LogstashMesosSlave(dockerClient, zooKeeper))
            .build());

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
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        cluster.addAndStartContainer(container);

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Framework framework = cluster.getStateInfo().getFramework("logstash");
            return
                framework != null && framework.getTasks().size() > 0 &&
                framework.getTasks().get(0).getState().equals("TASK_RUNNING");
        });
    }

}
