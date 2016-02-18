package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosContainer;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHitField;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.*;

/**
 * Tests whether the framework is deployed correctly
 */
@SuppressWarnings("Duplicates")
public class DeploymentSystemTest {

    private static DockerClient dockerClient = DockerClientFactory.build();

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentSystemTest.class);

    private MesosCluster cluster = new MesosCluster(new ClusterArchitecture.Builder()
            .withZooKeeper()
            .withMaster(zooKeeper -> new LogstashMesosMaster(dockerClient, zooKeeper))
            .withSlave(zooKeeper -> new LogstashMesosSlave(dockerClient, zooKeeper))
            .build());

    Optional<LogstashSchedulerContainer> scheduler = Optional.empty();

    @Before
    public void before() {
        cluster.start();
    }

    @After
    public void after() throws Exception {
        scheduler.ifPresent(scheduler -> {
            dockerClient.stopContainerCmd(scheduler.getContainerId()).withTimeout(30).exec();
        });

        await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            LOGGER.warn("Waiting");
            JSONArray frameworks = cluster.getClusterStateInfo().getJSONArray("frameworks");
            assertEquals(0, frameworks.length());
        });
        cluster.stop();
    }

    @Test
    public void testDeploymentDocker() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(true);
        cluster.addAndStartContainer(scheduler.get(), MesosContainer.DEFAULT_TIMEOUT_SEC);

        waitForFramework();
    }

    @Test
    public void testDeploymentJar() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(false);
        cluster.addAndStartContainer(scheduler.get(), MesosContainer.DEFAULT_TIMEOUT_SEC);

        waitForFramework();
    }

    private void waitForFramework() {
        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            JSONArray frameworks = cluster.getClusterStateInfo().getJSONArray("frameworks");
            if (frameworks.length() == 0) {
                LOGGER.info("Logstash framework is not yet running");
                return false;
            }

            JSONArray tasks = frameworks.getJSONObject(0).getJSONArray("tasks");
            if (tasks.length() != 0 && tasks.getJSONObject(0).getString("name").equals("logstash.task")) {
                LOGGER.info("Logstash executor running");
                return true;
            }

            LOGGER.info("Logstash executor not yet running");
            return false;
        });
    }

    @Test
    public void willForwardDataToElasticsearchInDockerMode() throws Exception {
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

            @Override
            public String getRole() {
                return "test";
            }
        };
        cluster.addAndStartContainer(elasticsearchInstance, MesosContainer.DEFAULT_TIMEOUT_SEC);

        final int elasticsearchPort = 9300;

        final AtomicReference<Client> elasticsearchClient = new AtomicReference<>();
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

        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, "logstash", elasticsearchInstance.getIpAddress() + ":9200"));
        scheduler.get().enableSyslog();
        cluster.addAndStartContainer(scheduler.get(), MesosContainer.DEFAULT_TIMEOUT_SEC);

        waitForFramework();

        final String sysLogPort = "514";
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        dockerClient.pullImageCmd("ubuntu:15.10").exec(new PullImageResultCallback()).awaitSuccess();
        final String logstashSlave = dockerClient.listContainersCmd().withSince(cluster.getSlaves()[0].getContainerId()).exec().stream().filter(container -> container.getImage().endsWith("/logstash-executor:latest")).findFirst().map(Container::getId).orElseThrow(() -> new AssertionError("Unable to find logstash container"));
        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            assertTrue(dockerClient.inspectContainerCmd(logstashSlave).exec().getState().isRunning());

            final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(logstashSlave, "logstash")).withCmd("logger", "--server=logstash", "--port=" + sysLogPort, "--udp", "--rfc3164", randomLogLine).exec();
            dockerClient.startContainerCmd(loggerContainer.getId()).exec();
            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                // TODO: this is a hack to determine whether the container has stopped.
                // We should use ...exec().getState().getRunning() but docker-java doesn't provide that
                // (even though it's available in the JSON provided by Docker).
                final String finishedAt = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getFinishedAt();
                assertNotNull(finishedAt);
                assertNotEquals("", finishedAt);
                assertNotEquals("0001-01-01T00:00:00Z", finishedAt);
            });
            final int exitCode = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getExitCode();
            dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
            assertEquals(0, exitCode);
            elasticsearchClient.get().prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").addField("mesos_agent_id").execute().actionGet().getHits().getAt(0).fields();
        });
        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            Map<String, SearchHitField> fields = elasticsearchClient.get().prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").addField("mesos_agent_id").execute().actionGet().getHits().getAt(0).fields();

            String esMessage = fields.get("message").getValue();
            assertEquals(randomLogLine, esMessage.trim());

            String esMesosSlaveId = fields.get("mesos_agent_id").getValue();

            String trueSlaveId;
            try {
                trueSlaveId = cluster.getClusterStateInfo().getJSONArray("slaves").getJSONObject(0).getString("id");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertEquals(
                    trueSlaveId,
                    esMesosSlaveId.trim()
            );
            return true;
        });
    }

    @Test
    public void willForwardDataToElasticsearchInJarMode() throws Exception {
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

            @Override
            public String getRole() {
                return "test";
            }
        };
        cluster.addAndStartContainer(elasticsearchInstance, MesosContainer.DEFAULT_TIMEOUT_SEC);

        final int elasticsearchPort = 9300;

        final AtomicReference<Client> elasticsearchClient = new AtomicReference<>();
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

        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, "logstash", elasticsearchInstance.getIpAddress() + ":9200"));
        scheduler.get().enableSyslog();
        scheduler.get().setDocker(false);
        cluster.addAndStartContainer(scheduler.get(), MesosContainer.DEFAULT_TIMEOUT_SEC);

        waitForFramework();

        final String sysLogPort = "514";
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        dockerClient.pullImageCmd("ubuntu:15.10").exec(new PullImageResultCallback()).awaitSuccess();
        final String logstashSlave = cluster.getSlaves()[0].getContainerId();
        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            assertTrue(dockerClient.inspectContainerCmd(logstashSlave).exec().getState().isRunning());

            final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(logstashSlave, "logstash")).withCmd("logger", "--server=logstash", "--port=" + sysLogPort, "--udp", "--rfc3164", randomLogLine).exec();
            dockerClient.startContainerCmd(loggerContainer.getId()).exec();
            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                // TODO: this is a hack to determine whether the container has stopped.
                // We should use ...exec().getState().getRunning() but docker-java doesn't provide that
                // (even though it's available in the JSON provided by Docker).
                final String finishedAt = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getFinishedAt();
                System.out.println("finishedAt = " + finishedAt);
                return StringUtils.isNotBlank(finishedAt) && !finishedAt.equals("0001-01-01T00:00:00Z");
            });
            final int exitCode = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getExitCode();
            dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
            assertEquals(0, exitCode);
            elasticsearchClient.get().prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").addField("mesos_agent_id").execute().actionGet().getHits().getAt(0).fields();
        });
        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            Map<String, SearchHitField> fields = elasticsearchClient.get().prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").addField("mesos_agent_id").execute().actionGet().getHits().getAt(0).fields();

            String esMessage = fields.get("message").getValue();
            assertEquals(randomLogLine, esMessage.trim());

            String esMesosSlaveId = fields.get("mesos_agent_id").getValue();

            String trueSlaveId;
            try {
                trueSlaveId = cluster.getClusterStateInfo().getJSONArray("slaves").getJSONObject(0).getString("id");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertEquals(
                    trueSlaveId,
                    esMesosSlaveId.trim()
            );
            return true;
        });
    }

    @Test
    public void willAddExecutorOnNewNodes() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(true);
        cluster.addAndStartContainer(scheduler.get(), MesosContainer.DEFAULT_TIMEOUT_SEC);

        waitForFramework();

        IntStream.range(0, 2).forEach(value -> cluster.addAndStartContainer(new LogstashMesosSlave(dockerClient, cluster.getZkContainer()), MesosContainer.DEFAULT_TIMEOUT_SEC));

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(
                () -> State.fromJSON(cluster.getClusterStateInfo().toString()).getFramework("logstash").getTasks().stream().filter(task -> task.getState().equals("TASK_RUNNING")).count() == 3
        );

        // TODO use com.containersol.minimesos.state.Task when it exposes the slave_id property https://github.com/ContainerSolutions/minimesos/issues/168
        JSONArray tasks = cluster.getClusterStateInfo().getJSONArray("frameworks").getJSONObject(0).getJSONArray("tasks");
        Set<String> slaveIds = new TreeSet<>();
        for (int i = 0; i < tasks.length(); i++) {
            slaveIds.add(tasks.getJSONObject(i).getString("slave_id"));
        }
        assertEquals(3, slaveIds.size());
    }

    @Test
    public void willStartNewExecutorIfOldExecutorFails() throws Exception {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();

        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, "logstash", null));
        cluster.addAndStartContainer(scheduler.get(), MesosContainer.DEFAULT_TIMEOUT_SEC);

        waitForFramework();

        final String firstSlaveId = cluster.getSlaves()[0].getContainerId();

        Function<String, Stream<Container>> getLogstashExecutorsSince = containerId -> dockerClient
                .listContainersCmd()
                .withSince(containerId)
                .exec()
                .stream()
                .filter(container -> container.getImage().endsWith("/logstash-executor:latest"));

        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            long count = getLogstashExecutorsSince.apply(firstSlaveId).count();
            LOGGER.info("There are " + count + " executors since " + firstSlaveId);
            assertEquals(1, count);
        });

        final String slaveToKillContainerId = getLogstashExecutorsSince.apply(firstSlaveId).findFirst().map(Container::getId).orElseThrow(() -> new RuntimeException("Unable to find logstash container"));

        dockerClient.killContainerCmd(slaveToKillContainerId).exec();

        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            assertEquals(1, getLogstashExecutorsSince.apply(slaveToKillContainerId).count());
        });
    }
}
