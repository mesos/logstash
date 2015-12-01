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
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Link;
import com.github.dockerjava.core.command.PullImageResultCallback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.await;
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
    public void willForwardDataToElasticsearch() throws JsonParseException, UnirestException, JsonMappingException, ExecutionException, InterruptedException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();

        final String elasticsearchClusterName = "test-" + System.currentTimeMillis();
        final AbstractContainer elasticsearchInstance = new AbstractContainer(dockerClient) {
            private final String version = "1.7";

            @Override
            protected void pullImage() {
                pullImage("elasticsearch", version);
            }

            @Override
            protected CreateContainerCmd dockerCommand() {
                return dockerClient.createContainerCmd("elasticsearch:" + version).withCmd("elasticsearch",  "-Des.cluster.name=\"" + elasticsearchClusterName + "\"", "-Des.discovery.zen.ping.multicast.enabled=false");
            }
        };
        cluster.addAndStartContainer(elasticsearchInstance);

        final int elasticsearchPort = 9300;

        AtomicReference<Client> elasticsearchClient = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            Client c = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", elasticsearchClusterName).build()).addTransportAddress(new InetSocketTransportAddress(elasticsearchInstance.getIpAddress(), elasticsearchPort));
            try {
                c.admin().cluster().health(Requests.clusterHealthRequest("_all")).actionGet();
            } catch (ElasticsearchException e) {
                c.close();
                return false;
            }
            elasticsearchClient.set(c);
            return true;
        });
        assertEquals(elasticsearchClusterName, elasticsearchClient.get().admin().cluster().health(Requests.clusterHealthRequest("_all")).actionGet().getClusterName());

        LogstashSchedulerContainer logstashSchedulerContainer = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, elasticsearchInstance.getIpAddress() + ":" + 9200);
        cluster.addAndStartContainer(logstashSchedulerContainer);

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Framework framework = cluster.getStateInfo().getFramework("logstash");
            return
                framework != null && framework.getTasks().size() > 0 &&
                framework.getTasks().get(0).getState().equals("TASK_RUNNING");
        });

        final String sysLogPort = "514";
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        PullImageResultCallback pullImageCallback = new PullImageResultCallback();
        dockerClient.pullImageCmd("ubuntu:15.10").exec(pullImageCallback);
        pullImageCallback.awaitSuccess();
        final String logstashSlave = dockerClient.listContainersCmd().withSince(cluster.getSlaves()[0].getContainerId()).exec().stream().filter(container -> container.getImage().equals("mesos/logstash-executor:latest")).findFirst().map(Container::getId).orElseThrow(() -> new RuntimeException("Unable to find logstash container"));
        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(logstashSlave, "logstash")).withCmd("logger", "--server", "logstash", "--rfc3164", "--tcp", "--port", sysLogPort, randomLogLine).exec();
            dockerClient.startContainerCmd(loggerContainer.getId()).exec();
            Thread.sleep(100L);
            await().atMost(1, TimeUnit.SECONDS).until(() -> StringUtils.isNotBlank(dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getFinishedAt()));
            final int exitCode = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getExitCode();
            dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
            return 0 == exitCode;
        });

        assertEquals(randomLogLine, getFirstMessageInLogstashIndex(elasticsearchClient.get()));
    }

    private String getFirstMessageInLogstashIndex(Client elasticsearchClient) {
        AtomicReference<String> message = new AtomicReference<>("NOT SET");
        await().atMost(10, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            try {
                String esMessage = elasticsearchClient.prepareSearch("logstash").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").execute().actionGet().getHits().getAt(0).fields().get("message").getValue();
                message.set(esMessage.trim());
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        return message.get();
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
