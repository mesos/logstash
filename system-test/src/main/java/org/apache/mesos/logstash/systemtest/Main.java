package org.apache.mesos.logstash.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;

/**
 * Main app to run Mesos Logstash with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Main {

    public static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        MesosClusterConfig config = MesosClusterConfig.builder()
                .numberOfSlaves(3)
                .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
                .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
                .build();

        MesosCluster cluster = new MesosCluster(config);
        cluster.start();

        LOGGER.info("Starting scheduler");
        LogstashSchedulerContainer scheduler = new LogstashSchedulerContainer(config.dockerClient, cluster.getMesosContainer().getIpAddress());
        scheduler.start();
        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":9092");

        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

}
