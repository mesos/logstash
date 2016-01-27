package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterArchitecture;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.state.Framework;
import com.containersol.minimesos.state.State;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.NotModifiedException;
import com.github.dockerjava.api.model.Container;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.*;

/**
 * Tests whether the framework is deployed correctly
 */
public class DeploymentSystemTest {

    private static DockerClient dockerClient = DockerClientFactory.build();

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentSystemTest.class);

    private MesosCluster cluster = new MesosCluster(new ClusterArchitecture.Builder()
            .withZooKeeper()
            .withMaster()
            .withSlave(zooKeeper -> new LogstashMesosSlave(dockerClient, zooKeeper))
            .build());

    Optional<LogstashSchedulerContainer> scheduler = Optional.empty();

    @Before
    public void before() {
        cluster.start();
    }

    @SuppressWarnings({"PMD.EmptyCatchBlock"})
    @After
    public void after() {
        try {
            scheduler.ifPresent(scheduler -> dockerClient.listContainersCmd().withSince(scheduler.getContainerId()).exec().stream()
                .filter(container -> Arrays.stream(container.getNames()).anyMatch(name -> name.startsWith("/mesos-")))
                .map(Container::getId)
                .peek(s -> System.out.println("Stopping mesos- container: " + s))
                .forEach(containerId -> dockerClient.stopContainerCmd(containerId).exec()));
        } catch (NotModifiedException e) {
            // Container is already stopped
        }
        cluster.stop();
    }

    protected State getClusterStateInfo() {
        try {
            return State.fromJSON(cluster.getStateInfoJSON().toString());
        } catch (Exception e) {
            fail(e.getMessage());
            return null;
        }
    }

    @Test
    public void testDeploymentDocker() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer schedulerContainer = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        schedulerContainer.setDocker(true);
        scheduler = Optional.of(schedulerContainer);
        cluster.addAndStartContainer(scheduler.get(), 60);

        waitForFramework();
    }

    @Test
    public void testDeploymentJar() throws JsonParseException, UnirestException,  JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer logstashSchedulerContainer = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        scheduler = Optional.of(logstashSchedulerContainer);
        cluster.addAndStartContainer(scheduler.get(), 60);

        waitForFramework();
    }

    private void waitForFramework() {
        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            JSONArray frameworks = cluster.getStateInfoJSON().getJSONArray("frameworks");
            if (frameworks.length() == 0) {
                LOGGER.info("Logstash framework is not yet running");
                return false;
            }

            JSONArray tasks = frameworks.getJSONObject(0).getJSONArray("tasks");
            if (tasks.length() != 0) {
                if (tasks.getJSONObject(0).getString("name").equals("logstash.task")) {
                    LOGGER.info("Logstash executor running");
                    return true;
                }
            }

            LOGGER.info("Logstash executor not yet running");
            return false;
        });
    }

    @Test
    public void willForwardDataToElasticsearch() throws IOException, UnirestException, ExecutionException, InterruptedException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();

        final ElasticsearchContainer elasticsearchContainer = new ElasticsearchContainer(dockerClient);
        cluster.addAndStartContainer(elasticsearchContainer, 60);

        elasticsearchContainer.waitUntilHealthy();

        LogstashSchedulerContainer logstashSchedulerContainer = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, elasticsearchContainer.getClientUrl());
        logstashSchedulerContainer.setDocker(true);
        scheduler = Optional.of(logstashSchedulerContainer);
        scheduler.get().enableSyslog();
        cluster.addAndStartContainer(scheduler.get(), 60);

        waitForFramework();

        String logline = "Hello " + RandomStringUtils.randomAlphanumeric(32);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    DatagramSocket socket = null;
                    socket = new DatagramSocket();
                    byte[] buf = logline.getBytes();
                    InetAddress address = InetAddress.getByName("localhost");
                    DatagramPacket packet = new DatagramPacket(buf, buf.length, address, 514);
                    socket.send(packet);
                } catch (IOException e) {
                    // Ignore
                }
            }
        };
        thread.run();

        await().atMost(2, TimeUnit.MINUTES).pollInterval(2, TimeUnit.SECONDS).until(() -> {
            SearchHits hits = elasticsearchContainer.getClient().prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").addField("mesos_slave_id").execute().actionGet().getHits();
            if (hits.totalHits() == 0) {
                LOGGER.info("Log message not found in Elasticsearch on " + elasticsearchContainer.getClientUrl());
                return false;
            }
            LOGGER.info("Log message found in Elasticsearch");
            return true;
        });

        SearchHits hits = elasticsearchContainer.getClient().prepareSearch("logstash-*").setQuery(QueryBuilders.simpleQueryStringQuery("hello")).addField("message").addField("mesos_slave_id").execute().actionGet().getHits();
        String esMessage = hits.getAt(0).getFields().get("message").getValue();
        assertEquals(logline, esMessage.trim());
        String esMesosSlaveId = hits.getAt(0).getFields().get("mesos_slave_id").getValue();
        String trueSlaveId = cluster.getStateInfoJSON().getJSONArray("slaves").getJSONObject(0).getString("id");
        assertEquals(trueSlaveId, esMesosSlaveId.trim());
    }

    @Test
    public void willAddExecutorOnNewNodes() throws JsonParseException, UnirestException, JsonMappingException {
        String zookeeperIpAddress = cluster.getZkContainer().getIpAddress();
        LogstashSchedulerContainer logstashSchedulerContainer = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress);
        logstashSchedulerContainer.setDocker(true);
        scheduler = Optional.of(logstashSchedulerContainer);
        cluster.addAndStartContainer(scheduler.get(), 60);

        waitForFramework();

        IntStream.range(0, 2).forEach(value -> cluster.addAndStartContainer(new LogstashMesosSlave(dockerClient, cluster.getZkContainer()), 60));

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

}
