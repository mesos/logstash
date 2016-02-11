package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.state.State;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Link;
import com.github.dockerjava.core.command.PullImageResultCallback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
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
    protected File tmpRoot = new File(".tmp");
    protected File tmpDir;

    @Before
    public void before() throws Exception {
        tmpDir = new File(tmpRoot, String.valueOf(System.currentTimeMillis()));
        FileUtils.forceMkdir(tmpDir);
        tmpRoot.deleteOnExit();
        cluster.start();
    }

    @After
    public void after() throws Exception {
        scheduler.ifPresent(scheduler -> dockerClient.stopContainerCmd(scheduler.getContainerId()).withTimeout(30).exec());

        await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            JSONArray frameworks = null;
            try {
                frameworks = cluster.getStateInfoJSON().getJSONArray("frameworks");
            } catch (UnirestException e) {
                fail("Couldn't get stateInfoJson: " + e.getMessage());
            }
            assertEquals(0, frameworks.length());
        });
        cluster.stop();

    }

    @Test
    public void testDeploymentDocker() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(true);
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();
    }

    @Test
    public void testDeploymentJar() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(false);
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();
    }

    @Test
    public void testDeploymentExternalConfiguration() throws Exception {
        final File logstashConfig = new File(tmpDir, "logstash.config");
        FileUtils.writeStringToFile(logstashConfig, "input { heartbeat {} } output {}");

        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(false);
        scheduler.get().setLogstashConfig(logstashConfig);
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();
    }

    private void waitForFramework() {
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            JSONArray frameworks = cluster.getStateInfoJSON().getJSONArray("frameworks");
            if (frameworks.length() == 0) {
                LOGGER.info("Logstash framework is not yet running");
                return false;
            }

            JSONArray tasks = frameworks.getJSONObject(0).getJSONArray("tasks");

            if (IntStream.range(0, tasks.length()).mapToObj(tasks::getJSONObject).filter(task -> task.getString("name").equals("logstash.task")).anyMatch(task -> task.getString("state").equalsIgnoreCase("TASK_RUNNING"))) {
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

        final ElasticsearchContainer elasticsearchInstance = new ElasticsearchContainer(dockerClient);
        cluster.addAndStartContainer(elasticsearchInstance);

        Client elasticsearchClient = elasticsearchInstance.createClient();

        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, "logstash", elasticsearchInstance.getIpAddress() + ":9200"));
        scheduler.get().enableSyslog();
        scheduler.get().setDocker(true);
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();

        final String sysLogPort = "514";
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        dockerClient.pullImageCmd("ubuntu:15.10").exec(new PullImageResultCallback()).awaitSuccess();
        final String logstashSlave = dockerClient.listContainersCmd().withSince(cluster.getSlaves()[0].getContainerId()).exec().stream().filter(container -> container.getImage().endsWith("/logstash-executor:latest")).findFirst().map(Container::getId).orElseThrow(() -> new AssertionError("Unable to find logstash container"));

        assertTrue(dockerClient.inspectContainerCmd(logstashSlave).exec().getState().isRunning());

        final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(logstashSlave, "logstash")).withCmd("logger", "--server=logstash", "--port=" + sysLogPort, "--udp", "--rfc3164", randomLogLine).exec();
        dockerClient.startContainerCmd(loggerContainer.getId()).exec();
        final String finishedAt = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getFinishedAt();
        assertNotEquals("", finishedAt.trim());
        assertNotEquals("0001-01-01T00:00:00Z", finishedAt);

        final int exitCode = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getExitCode();
        dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
        assertEquals(0, exitCode);

        await().atMost(10, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            final SearchHits hits = elasticsearchClient.prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("Hello")).addField("message").addField("mesos_agent_id").execute().actionGet().getHits();
            assertEquals(1, hits.totalHits());
            Map<String, SearchHitField> fields = hits.getAt(0).fields();

            String esMessage = fields.get("message").getValue();
            assertEquals(randomLogLine, esMessage.trim());

            String esMesosSlaveId = fields.get("mesos_agent_id").getValue();

            String trueSlaveId;
            try {
                trueSlaveId = cluster.getStateInfoJSON().getJSONArray("slaves").getJSONObject(0).getString("id");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertEquals(trueSlaveId, esMesosSlaveId.trim());
        });
    }

    @Test
    public void willForwardDataToElasticsearchInJarMode() throws Exception {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();

        final ElasticsearchContainer elasticsearchInstance = new ElasticsearchContainer(dockerClient);
        cluster.addAndStartContainer(elasticsearchInstance);

        Client elasticsearchClient = elasticsearchInstance.createClient();

        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, "logstash", elasticsearchInstance.getIpAddress() + ":9200"));
        scheduler.get().enableSyslog();
        scheduler.get().setDocker(false);
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();

        final String sysLogPort = "514";
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        dockerClient.pullImageCmd("ubuntu:15.10").exec(new PullImageResultCallback()).awaitSuccess();
        final String logstashSlave = cluster.getSlaves()[0].getContainerId();

        assertTrue(dockerClient.inspectContainerCmd(logstashSlave).exec().getState().isRunning());

        final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(logstashSlave, "logstash")).withCmd("logger", "--server=logstash", "--port=" + sysLogPort, "--udp", "--rfc3164", randomLogLine).exec();
        dockerClient.startContainerCmd(loggerContainer.getId()).exec();
        final String finishedAt = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getFinishedAt();
        assertNotEquals("", finishedAt.trim());
        assertNotEquals("0001-01-01T00:00:00Z", finishedAt);

        final int exitCode = dockerClient.inspectContainerCmd(loggerContainer.getId()).exec().getState().getExitCode();
        dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
        assertEquals(0, exitCode);

        await().atMost(10, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            final SearchHits hits = elasticsearchClient.prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("Hello")).addField("message").addField("mesos_agent_id").execute().actionGet().getHits();
            assertEquals(1, hits.totalHits());
            Map<String, SearchHitField> fields = hits.getAt(0).fields();

            String esMessage = fields.get("message").getValue();
            assertEquals(randomLogLine, esMessage.trim());

            String esMesosSlaveId = fields.get("mesos_agent_id").getValue();

            String trueSlaveId;
            try {
                trueSlaveId = cluster.getStateInfoJSON().getJSONArray("slaves").getJSONObject(0).getString("id");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertEquals(trueSlaveId, esMesosSlaveId.trim());
        });
    }

    @Test
    public void willAddExecutorOnNewNodes() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        scheduler = Optional.of(new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, null, null));
        scheduler.get().setDocker(true);
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();

        IntStream.range(0, 2).forEach(value -> cluster.addAndStartContainer(new LogstashMesosSlave(dockerClient, cluster.getZkContainer())));

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(
                () -> State.fromJSON(cluster.getStateInfoJSON().toString()).getFramework("logstash").getTasks().stream().filter(task -> task.getState().equals("TASK_RUNNING")).count() == 3
        );

        // TODO use com.containersol.minimesos.state.Task when it exposes the slave_id property https://github.com/ContainerSolutions/minimesos/issues/168
        JSONArray tasks = cluster.getStateInfoJSON().getJSONArray("frameworks").getJSONObject(0).getJSONArray("tasks");
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
        cluster.addAndStartContainer(scheduler.get());

        waitForFramework();

        Function<String, Stream<Container>> getLogstashExecutorsSince = containerId -> dockerClient
                .listContainersCmd()
                .withSince(containerId)
                .exec()
                .stream()
                .filter(container -> container.getImage().endsWith("/logstash-executor:latest"));

        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            long count = getLogstashExecutorsSince.apply(cluster.getSlaves()[0].getContainerId()).count();
            LOGGER.info("There are " + count + " executors since " + cluster.getSlaves()[0].getContainerId());
            assertEquals(1, count);
        });

        final String slaveToKillContainerId = getLogstashExecutorsSince.apply(cluster.getSlaves()[0].getContainerId()).findFirst().map(Container::getId).orElseThrow(() -> new RuntimeException("Unable to find logstash container"));

        dockerClient.killContainerCmd(slaveToKillContainerId).exec();

        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            assertEquals(1, getLogstashExecutorsSince.apply(slaveToKillContainerId).count());
        });
    }
}
