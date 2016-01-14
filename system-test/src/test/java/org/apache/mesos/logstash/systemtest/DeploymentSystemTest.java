package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.state.Framework;
import com.containersol.minimesos.state.State;
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

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.*;

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

    Optional<LogstashSchedulerContainer> scheduler = Optional.empty();

    @Before
    public void before() {
        cluster.start();
    }

    @After
    public void after() {
        scheduler.ifPresent(scheduler -> dockerClient.listContainersCmd().withSince(scheduler.getContainerId()).exec().stream()
                .filter(container -> Arrays.stream(container.getNames()).anyMatch(name -> name.startsWith("/mesos-")))
                .map(Container::getId)
                .peek(s -> System.out.println("Stopping mesos- container: " + s))
                .forEach(containerId -> dockerClient.stopContainerCmd(containerId).exec()));
        cluster.stop();
    }

    protected State getClusterStateInfo() {
        try {
            return cluster.getStateInfo();
        } catch (Exception e) {
            fail(e.getMessage());
            return null;
        }
    }

    @Test
    public void testDeployment() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        cluster.addAndStartContainer(container);

        waitForFramework();
    }

    private void waitForFramework() {
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Framework framework = getClusterStateInfo().getFramework("logstash");
            assertNotNull(framework);
            assertTrue(framework.getTasks().size() > 0);
            assertEquals("TASK_RUNNING", framework.getTasks().get(0).getState());
        });
    }

    @Test
    public void willForwardDataToElasticsearch() throws JsonParseException, UnirestException, JsonMappingException, ExecutionException {
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

        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, elasticsearchInstance.getIpAddress() + ":" + 9200));
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();

        final String sysLogPort = "10514";
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        dockerClient.pullImageCmd("ubuntu:15.10").exec(new PullImageResultCallback()).awaitSuccess();
        final String logstashSlave = dockerClient.listContainersCmd().withSince(cluster.getSlaves()[0].getContainerId()).exec().stream().filter(container -> container.getImage().endsWith("/logstash-executor:latest")).findFirst().map(Container::getId).orElseThrow(() -> new RuntimeException("Unable to find logstash container"));
        await().atMost(2, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            assertTrue(dockerClient.inspectContainerCmd(logstashSlave).exec().getState().isRunning());

            final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(logstashSlave, "logstash")).withCmd("logger", "--server=logstash", "--port=" + sysLogPort, "--udp", "--rfc3164", randomLogLine).exec();
            dockerClient.startContainerCmd(loggerContainer.getId()).exec();
            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                // TODO: this is a hack to determine whether the container has stopped.
                // We should use ...exec().getState().getRunning() but docker-java doesn't provide that
                // (even though it's available in the JSON provided by Docker).
                final String finishedAt = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getFinishedAt();
                return StringUtils.isNotBlank(finishedAt) && !finishedAt.equals("0001-01-01T00:00:00Z");
            });
            final int exitCode = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getExitCode();
            dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
            assertEquals(0, exitCode);
            assertEquals(randomLogLine, getFirstMessageInLogstashIndex(elasticsearchClient.get()));
        });
    }

    private String getFirstMessageInLogstashIndex(Client elasticsearchClient) {
        try {
            String esMessage = elasticsearchClient.prepareSearch("logstash").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").execute().actionGet().getHits().getAt(0).fields().get("message").getValue();
            return esMessage.trim();
        } catch (Exception e) {
            return "NOT FOUND";
        }
    }

    @Test
    public void willAddExecutorOnNewNodes() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer container = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        cluster.addAndStartContainer(container);

        waitForFramework();

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
