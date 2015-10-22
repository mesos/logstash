package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import org.apache.log4j.Logger;

/**
 * Main app to run Mesos Logstash with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Main {

    public static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        MesosCluster cluster = new MesosCluster(MesosClusterConfig.builder()
                .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
                .build());
        cluster.start();

        LOGGER.info("Starting scheduler");
//        LOGGER.info();
        LogstashSchedulerContainer scheduler = new LogstashSchedulerContainer(cluster.getConfig().dockerClient, cluster.getZkContainer().getIpAddress());
        cluster.addAndStartContainer(scheduler);
        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":9092");

        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }
}