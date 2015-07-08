package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import org.apache.mesos.logstash.scheduler.Scheduler;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.MesosClusterConfig;
import org.apache.mesos.mini.util.DockerUtil;
import org.junit.*;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

import java.io.File;

/**
 * Created by peldan on 07/07/15.
 */
public class MessageTest {

    public static class SmallInnerClass extends ExternalResource {
        // For usage as JUnit rule...
        @Override
        protected void before() throws Throwable {
            DockerUtil utils = new DockerUtil(CONFIG.dockerClient);

            utils.buildImageFromFolder(new File("../executor"), "logstash-executor");
        }
    }

    public static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            // Note: Logstash-mesos uses container discovery, and mesos-local runs all
            // the executors in the same docker host. So it is safest to just use 1 slave for now..
            .numberOfSlaves(1)
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]"})
            .dockerInDockerImages(new String[]{"logstash-executor"}).build();

    public static MesosCluster cluster = new MesosCluster(CONFIG);

    @ClassRule
    public static RuleChain chain= RuleChain
            .outerRule(new SmallInnerClass()).around(cluster);


    @BeforeClass
    public static void startScheduler() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                new Scheduler(cluster.getMesosMasterURL(), "logstash-executor").run();
            }
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

        cluster.getStateInfo();

        System.out.println("HELLO WORLD");
        //Scheduler scheduler = new Scheduler(cluster.getMasterUrl(), "epeld/logstash-executor");
        //scheduler.run();

        //
    }

    @Test
    public void schedulerStarts2() throws Exception {

        cluster.getStateInfo();

        System.out.println("HELLO WORLD2");
        //Scheduler scheduler = new Scheduler(cluster.getMasterUrl(), "epeld/logstash-executor");
        //scheduler.run();

        //
    }

}
