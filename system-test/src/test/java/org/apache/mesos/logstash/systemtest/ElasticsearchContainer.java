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
import static org.junit.Assert.assertEquals;

/**
 * Container running Elasticsearch
 */
public class ElasticsearchContainer extends AbstractContainer {
    public static final String VERSION = "1.7";

    private String elasticsearchClusterName;

    public ElasticsearchContainer(DockerClient dockerClient, String elasticsearchClusterName) {
        super(dockerClient);
        this.elasticsearchClusterName = elasticsearchClusterName;
    }

    public ElasticsearchContainer(DockerClient dockerClient) {
        this(dockerClient, "test-" + System.currentTimeMillis());
    }

    @Override
    protected void pullImage() {
        pullImage("elasticsearch", VERSION);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient.createContainerCmd("elasticsearch:" + VERSION).withCmd("elasticsearch",  "-Des.cluster.name=\"" + elasticsearchClusterName + "\"", "-Des.discovery.zen.ping.multicast.enabled=false");
    }


    public Client createClient() {
        final AtomicReference<Client> elasticsearchClient = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).until(() -> {
            Client c = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", elasticsearchClusterName).build()).addTransportAddress(new InetSocketTransportAddress(getIpAddress(), 9300));
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

    @Override
    public String getRole() {
        return "elasticsearch";
    }

}
