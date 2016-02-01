package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.jayway.awaitility.Awaitility.await;

/**
 * Container running Elasticsearch
 */
public class ElasticsearchContainer extends AbstractContainer {

    public static final int CLIENT_PORT = 9200;
    public static final int TRANSPORT_PORT = 9300;
    public static final String VERSION = "1.7";
    public static final String CLUSTER_NAME = "test-" + System.currentTimeMillis();

    private AtomicReference<Client> client;

    public ElasticsearchContainer(DockerClient dockerClient) {
        super(dockerClient);
    }

    @Override
    protected void pullImage() {
        pullImage("elasticsearch", VERSION);
    }

    @Override
    public String getName() {
        return "elasticsearch-" + CLUSTER_NAME;
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd("elasticsearch:" + VERSION).withCmd("elasticsearch", "-Des.cluster.name=\"" + CLUSTER_NAME + "\"", "-Des.discovery.zen.ping.multicast.enabled=false")
                           .withName(getName());
    }

    public void waitUntilHealthy() {
        client = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            Client c = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", CLUSTER_NAME).build()).addTransportAddress(new InetSocketTransportAddress(getIpAddress(), TRANSPORT_PORT));
            try {
                c.admin().cluster().health(Requests.clusterHealthRequest("_all")).actionGet();
            } catch (ElasticsearchException e) {
                c.close();
                return false;
            }
            client.set(c);
            return true;
        });
    }

    public Client getClient() {
        return client.get();
    }

    public String getClientUrl() {
        return "http://" + getIpAddress() + ":" + CLIENT_PORT;
    }
}
