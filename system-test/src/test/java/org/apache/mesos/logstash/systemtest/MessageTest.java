package org.apache.mesos.logstash.systemtest;

import org.apache.mesos.logstash.scheduler.Scheduler;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.MesosClusterConfig;
import org.apache.mesos.mini.state.Framework;
import org.apache.mesos.mini.state.State;
import org.junit.*;

import java.io.File;

/**
 * Created by peldan on 07/07/15.
 */
public class MessageTest {

    public static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            // Note: Logstash-mesos uses container discovery, and mesos-local runs all
            // the executors in the same docker host. So it is safest to just use 1 slave for now..
            .numberOfSlaves(1)
            .imagesToBuild(new MesosClusterConfig.ImageToBuild(new File("../executor"), "logstash-executor"))
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]"})
            .dockerInDockerImages(new String[]{"logstash-executor"}).build();

    @ClassRule
    public static MesosCluster cluster = new MesosCluster(CONFIG);

    @BeforeClass
    public static void startScheduler() {
        Thread t = new Thread(() -> {
            new Scheduler(cluster.getMesosMasterURL(), "logstash-executor").run();
        });
        t.setDaemon(true);
        t.start();
    }

    @AfterClass
    public static void stopScheduler() {
        // TODO
    }

    @Test
    public void schedulerStarts() throws Exception {
        // TODO await startup

        State state = cluster.getStateInfo();
        Framework logstash = state.getFramework("logstash");

        Assert.assertNotNull(logstash);
    }

    @Test
    public void schedulerStarts2() throws Exception {

        cluster.getStateInfoJSON();

        System.out.println("HELLO WORLD2");
        //Scheduler scheduler = new Scheduler(cluster.getMasterUrl(), "epeld/logstash-executor");
        //scheduler.run();

        //
    }

}
