package org.apache.mesos.logstash.systemtest;

import org.apache.mesos.logstash.scheduler.Scheduler;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.MesosClusterConfig;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.Socket;

/**
 * Created by peldan on 07/07/15.
 */
public class MessageTest {

    // Creates a destination where we can instruct logstash to forward the logs
//    private Socket createLogstashBackend() {
//        Inet4Address inet4addr = new Inet4Address();
//        return new Socket(inet4addr, rand);
//    }

    @ClassRule
    public static MesosCluster cluster = new MesosCluster(MesosClusterConfig.builder()
            .numberOfSlaves(1)
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]"})
            .dockerInDockerImages(new String[]{"epeld/logstash-executor"}).build());

    @Test
    public void schedulerStarts() throws Exception {

        System.out.println("HELLO WORLD");
        //Scheduler scheduler = new Scheduler(cluster.getMasterUrl(), "epeld/logstash-executor");
        //scheduler.run();

        //
    }

}
