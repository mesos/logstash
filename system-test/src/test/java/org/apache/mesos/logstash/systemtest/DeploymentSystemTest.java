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
import com.github.dockerjava.api.command.InspectContainerResponse;
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
    public void willForwardSyslogLogsToElasticsearch() throws JsonParseException, UnirestException, JsonMappingException, ExecutionException, InterruptedException {
        String elasticsearchClusterName = "test-" + System.currentTimeMillis();
        final AbstractContainer elasticsearchInstance = startElasticsearchClusterWithName(elasticsearchClusterName);
        Client elasticsearchClient = elasticsearchClientForCluster(elasticsearchInstance, elasticsearchClusterName);
        startLogstash(cluster.getZkContainer().getIpAddress(), elasticsearchInstance);
        final String randomLogLine = "Hello " + RandomStringUtils.randomAlphanumeric(32);
        sendSyslogEvent(getLogstashExecutor(), randomLogLine);
        assertEquals(randomLogLine, getFirstMessageInLogstashIndex(elasticsearchClient));
    }

    @Test
    public void willForwardCollectdLogsToElasticsearch() {
        String elasticsearchClusterName = "test-" + System.currentTimeMillis();
        final AbstractContainer elasticsearchInstance = startElasticsearchClusterWithName(elasticsearchClusterName);
        Client elasticsearchClient = elasticsearchClientForCluster(elasticsearchInstance, elasticsearchClusterName);
        startLogstash(cluster.getZkContainer().getIpAddress(), elasticsearchInstance);
        CreateContainerResponse collectdClientContainer = createCollectdClientSendingIrqLogsToCollectdServer(getLogstashExecutor());
        waitForCollectdIrqEventsInElasticsearch(elasticsearchClient);
        dockerClient.removeContainerCmd(collectdClientContainer.getId()).exec();
    }

    private CreateContainerResponse createCollectdClientSendingIrqLogsToCollectdServer(Container collectdServerContainer) {
        final String collectdConf =
                // Watch for interrupt request events ...
                "LoadPlugin irq\n" +

                // ... and log those events to our collectd server
                "LoadPlugin \"network\"\n" +
                "<Plugin \"network\">\n" +
                "  Server \"collectdserver\" \"25827\"\n" +
                "</Plugin>\n";

        return dockerClient
                .createContainerCmd("jhftrifork/docker-collectd:latest")
                .withLinks(new Link(collectdServerContainer.getId(), "collectdserver"))
                .withEnv("COLLECTD_CONF=" + collectdConf)
                .exec();
    }

    private void waitForCollectdIrqEventsInElasticsearch(Client elasticsearchClient) {
        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            try {
                long numberOfIrqEventsInElasticsearch = elasticsearchClient
                        .prepareSearch("logstash")
                        .setQuery(QueryBuilders.matchQuery("plugin", "irq"))
                        .addField("message")
                        .execute()
                        .actionGet()
                        .getHits()
                        .getTotalHits();
                return numberOfIrqEventsInElasticsearch > 0;
            } catch (Exception e) {
                return false;
            }
        });
    }

    private AbstractContainer startElasticsearchClusterWithName(String elasticsearchClusterName) {
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
        return elasticsearchInstance;
    }

    private Client elasticsearchClientForCluster(AbstractContainer elasticsearchInstance, String elasticsearchClusterName) {
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

        return elasticsearchClient.get();
    }

    private void startLogstash(String zookeeperIpAddress, AbstractContainer elasticsearchInstance) {
        LogstashSchedulerContainer logstashSchedulerContainer = new LogstashSchedulerContainer(dockerClient, zookeeperIpAddress, elasticsearchInstance.getIpAddress() + ":" + 9200);
        cluster.addAndStartContainer(logstashSchedulerContainer);

        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Framework framework = cluster.getStateInfo().getFramework("logstash");
            return
                    framework != null && framework.getTasks().size() > 0 &&
                            framework.getTasks().get(0).getState().equals("TASK_RUNNING");
        });
    }

    private boolean containerHasStopped(String containerId) {
        // TODO: this is a hack to determine whether the container has stopped.
        // We should use ...exec().getState().getRunning() but docker-java doesn't provide that
        // (even though it's available in the JSON provided by Docker).
        final String finishedAt = dockerClient.inspectContainerCmd(containerId).exec().getState().getFinishedAt();
        return StringUtils.isNotBlank(finishedAt) && !finishedAt.equals("0001-01-01T00:00:00Z");
    }

    private InspectContainerResponse.ContainerState waitForContainerToStop(String containerId) {
        await().atMost(10, TimeUnit.SECONDS).until(() -> containerHasStopped(containerId));
        return dockerClient.inspectContainerCmd(containerId).exec().getState();
    }

    private void pullImage(String repository) {
        PullImageResultCallback pullImageCallback = new PullImageResultCallback();
        dockerClient.pullImageCmd(repository).exec(pullImageCallback);
        pullImageCallback.awaitSuccess();
    }

    private void sendSyslogEvent(Container syslogContainer, String logLine) {
        final String sysLogPort = "514";

        pullImage("ubuntu:15.10");

        await().atMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            final CreateContainerResponse loggerContainer = dockerClient.createContainerCmd("ubuntu:15.10").withLinks(new Link(syslogContainer.getId(), "syslogserver")).withCmd("logger", "--server", "syslogserver", "--rfc3164", "--tcp", "--port", sysLogPort, logLine).exec();
            dockerClient.startContainerCmd(loggerContainer.getId()).exec();
            Thread.sleep(100L);
            final int exitCode = waitForContainerToStop(loggerContainer.getId()).getExitCode();
            dockerClient.removeContainerCmd(loggerContainer.getId()).exec();
            return 0 == exitCode;
        });
    }

    private Container getLogstashExecutor() {
        return dockerClient
                .listContainersCmd()
                .withSince(cluster.getSlaves()[0].getContainerId())
                .exec()
                .stream()
                .filter(container -> container.getImage().equals("mesos/logstash-executor:latest"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unable to find logstash container"));
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
