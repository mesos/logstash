package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterUtil;
import com.containersol.minimesos.mesos.MesosSlave;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;

import java.util.List;
import java.util.TreeMap;

@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class LocalCluster {

    private static final String DOCKER_PORT = "2376";
    public final MesosCluster cluster = new MesosCluster(ClusterUtil.withSlaves(3, zooKeeper -> new MesosSlave(null, zooKeeper) {
        @Override
        public TreeMap<String, String> getDefaultEnvVars() {
            final TreeMap<String, String> envVars = super.getDefaultEnvVars();
            envVars.put("MESOS_RESOURCES", "ports(*):[9299-9299,9300-9300]");
            return envVars;
        }
    }).withMaster().withZooKeeper().build());

    public static void main(String[] args) throws Exception {
        new LocalCluster().run();
    }

    private void run() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
        cluster.start();

        DockerClientConfig.DockerClientConfigBuilder dockerConfigBuilder = DockerClientConfig
            .createDefaultConfigBuilder()
            .withUri("http://" + cluster.getMesosMasterContainer().getIpAddress() + ":" + DOCKER_PORT);
        DockerClient clusterDockerClient = DockerClientBuilder
            .getInstance(dockerConfigBuilder.build()).build();

        DummyFrameworkContainer dummyFrameworkContainer = new DummyFrameworkContainer(
            clusterDockerClient, "dummy-framework");
        dummyFrameworkContainer.start();

        System.setProperty("mesos.zk", cluster.getZkUrl());
        System.setProperty("mesos.logstash.logstash.heap.size", "128");
        System.setProperty("mesos.logstash.executor.heap.size", "64");

        System.out.println("");
        System.out.println("Cluster Started.");
        System.out.println("MASTER URL: " + cluster.getMesosMasterContainer().getFormattedZKAddress());
        System.out.println("");

        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(5000);
            printRunningContainers(clusterDockerClient);
        }
    }

    private void printRunningContainers(DockerClient dockerClient) {
        List<Container> containers = dockerClient.listContainersCmd().exec();
        for (Container container : containers) {
            System.out.println(container.getImage());
        }
    }

}
