package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterUtil;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.github.dockerjava.api.DockerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Main app to run Mesos Logstash with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        DockerClient dockerClient = DockerClientFactory.build();

        MesosCluster cluster = new MesosCluster(ClusterUtil.withSlaves(1, zooKeeper -> new LogstashMesosSlave(dockerClient, zooKeeper)).withMaster().build());

        cluster.start();

/*
        LOGGER.info("Starting scheduler");
        LogstashSchedulerContainer scheduler = new LogstashSchedulerContainer(dockerClient, cluster.getZkContainer().getIpAddress());
        scheduler.start();
        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":9092");
*/

        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

}
