package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.ClusterUtil;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosSlave;
import com.github.dockerjava.api.DockerClient;
import org.apache.log4j.Logger;
import org.junit.Ignore;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main app to run Mesos Logstash with Mini Mesos.
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
@Ignore
public class Main {


    public static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        DockerClient dockerClient = DockerClientFactory.build();

        MesosCluster cluster = new MesosCluster(ClusterUtil.withSlaves(3, zooKeeper -> new MesosSlave(dockerClient, zooKeeper) {
            @Override
            public TreeMap<String, String> getDefaultEnvVars() {
                final TreeMap<String, String> envVars = super.getDefaultEnvVars();
                envVars.put("MESOS_RESOURCES", "ports(*):[9299-9299,9300-9300]");
                return envVars;
            }
        }).withMaster().build());

        cluster.start();

        LOGGER.info("Starting scheduler");
        LogstashSchedulerContainer scheduler = new LogstashSchedulerContainer(dockerClient, cluster.getMesosMasterContainer().getIpAddress());
        scheduler.start();
        LOGGER.info("Scheduler started at http://" + scheduler.getIpAddress() + ":9092");

        LOGGER.info("Type CTRL-C to quit");
        while (true) {
            Thread.sleep(1000);
        }
    }

}
