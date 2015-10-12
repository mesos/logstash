package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.*;

/**
 * Tests whether the framework is deployed correctly
 */
public class DeploymentSystemTest {

    private MesosClusterConfig config = MesosClusterConfig.builder().numberOfSlaves(1)
            .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
            .build();

    public MesosCluster cluster = new MesosCluster(config);

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
        String mesosIpAddress = cluster.getMesosMasterContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(config.dockerClient, mesosIpAddress);
        cluster.addAndStartContainer(container);

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return
                        cluster.getStateInfo().getFramework("logstash") != null &&
                                cluster.getStateInfo().getFramework("logstash").getTasks().get(0).getState().equals("TASK_RUNNING");
            }
        });
    }

}
