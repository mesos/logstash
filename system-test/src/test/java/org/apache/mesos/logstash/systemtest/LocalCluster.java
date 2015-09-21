package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;

import java.util.List;

@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class LocalCluster {

    private static final String DOCKER_PORT = "2376";
    public final MesosCluster cluster = MesosClusterConfig.builder()
        .numberOfSlaves(1)
        .privateRegistryPort(3333)
        .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
        .build();

    public static void main(String[] args) throws Exception {
        new LocalCluster().run();
    }

    private void run() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
        cluster.start();

        DockerClientConfig.DockerClientConfigBuilder dockerConfigBuilder = DockerClientConfig
            .createDefaultConfigBuilder()
            .withUri("http://" + cluster.getMesosContainer().getIpAddress() + ":" + DOCKER_PORT);
        DockerClient clusterDockerClient = DockerClientBuilder
            .getInstance(dockerConfigBuilder.build()).build();

        cluster.injectImage("mesos/logstash-executor");

        DummyFrameworkContainer dummyFrameworkContainer = new DummyFrameworkContainer(
            clusterDockerClient, "dummy-framework");
        dummyFrameworkContainer.start();

        String zkAddress = cluster.getMesosContainer().getIpAddress() + ":2181";

        System.setProperty("mesos.zk", "zk://" + zkAddress + "/mesos");
        System.setProperty("mesos.logstash.logstash.heap.size", "128");
        System.setProperty("mesos.logstash.executor.heap.size", "64");

        System.out.println("");
        System.out.println("Cluster Started.");
        System.out.println("MASTER URL: " + cluster.getMesosContainer().getMesosMasterURL());
        System.out.println("");

        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(5000);
            printRunningContainers();
        }
    }

    private void printRunningContainers() {
        DockerClient dockerClient = cluster.getConfig().dockerClient;

        List<Container> containers = dockerClient.listContainersCmd().exec();
        for (Container container : containers) {
            System.out.println(container.getImage());
        }
    }

}
