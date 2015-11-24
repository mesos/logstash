package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.state.Framework;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.*;
import static org.junit.Assert.assertEquals;

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
    @Test
    public void willForwardDataToElasticsearch() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        cluster.addAndStartContainer(container);

        final String clusterName = "test-" + System.currentTimeMillis();
        final AbstractContainer elasticsearchInstance = new AbstractContainer(dockerClient) {
            private final String version = "1.7";

            @Override
            protected void pullImage() {
                pullImage("elasticsearch", version);
            }

            @Override
            protected CreateContainerCmd dockerCommand() {
                return dockerClient.createContainerCmd("elasticsearch:" + version).withCmd("elasticsearch",  "-Des.cluster.name=\"" + clusterName + "\"");
            }
        };
        cluster.addAndStartContainer(elasticsearchInstance);

        AtomicReference<Client> client = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            Client c = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build()).addTransportAddress(new InetSocketTransportAddress(elasticsearchInstance.getIpAddress(), 9300));
            try {
                c.admin().cluster().health(Requests.clusterHealthRequest("_all")).actionGet();
            } catch (ElasticsearchException e) {
                c.close();
                return false;
            }
            client.set(c);
            return true;
        });
        assertEquals(clusterName, client.get().admin().cluster().health(Requests.clusterHealthRequest("_all")).actionGet().getClusterName());

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Framework framework = cluster.getStateInfo().getFramework("logstash");
            return
                framework != null && framework.getTasks().size() > 0 &&
                framework.getTasks().get(0).getState().equals("TASK_RUNNING");
        });
        // TODO: 18/11/2015 Log something through logstash
        // TODO: 18/11/2015 Look for a statement in ES
    }

    @Test
    public void willAddExecutorOnNewNodes() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        cluster.addAndStartContainer(container);

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Framework framework = cluster.getStateInfo().getFramework("logstash");
            return
                framework != null && framework.getTasks().size() > 0 &&
                framework.getTasks().get(0).getState().equals("TASK_RUNNING");
        });

        IntStream.range(0, 2).forEach(value -> cluster.addAndStartContainer(new LogstashMesosSlave(dockerClient, cluster.getZkContainer())));

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(
                () -> cluster.getStateInfo().getFramework("logstash").getTasks().stream().filter(task -> task.getState().equals("TASK_RUNNING")).count() == 3
        );

        // TODO use com.containersol.minimesos.state.Task when it exposes the slave_id property https://github.com/ContainerSolutions/minimesos/issues/168
        JSONArray tasks = cluster.getStateInfoJSON().getJSONArray("frameworks").getJSONObject(0).getJSONArray("tasks");
        Set<String> slaveIds = new TreeSet<>();
        for (int i = 0; i < tasks.length(); i++) {
            slaveIds.add(tasks.getJSONObject(i).getString("slave_id"));
        }
        assertEquals(3, slaveIds.size());
    }

}
