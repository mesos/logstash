package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterUtil;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import org.apache.log4j.Logger;

/**
 * Main app to run Mesos Logstash with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Main {


    public static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        DockerClient dockerClient = DockerClientFactory.build();

        MesosCluster cluster = new MesosCluster(ClusterUtil.withSlaves(3, zooKeeper -> new LogstashMesosSlave(dockerClient, zooKeeper)).withMaster().build());

        cluster.start();

        LOGGER.info("Starting scheduler");
        LogstashSchedulerContainer scheduler = new LogstashSchedulerContainer(dockerClient, cluster.getZkContainer().getIpAddress());
        scheduler.start();
        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":9092");

        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

}
