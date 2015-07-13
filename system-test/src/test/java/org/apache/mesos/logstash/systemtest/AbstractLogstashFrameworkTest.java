package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.InternalServerErrorException;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.scheduler.Scheduler;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.apache.mesos.mini.state.State;
import org.apache.mesos.mini.util.Predicate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;


public abstract class AbstractLogstashFrameworkTest {
    public static final MesosClusterConfig config = MesosClusterConfig.builder()
            // Note: Logstash-mesos uses container discovery, and mesos-local runs all
            // the executors in the same docker host. So it is safest to just use 1 slave for now..
            .numberOfSlaves(1)
//            .imagesToBuild(new MesosClusterConfig.ImageToBuild(new File("../executor"), "logstash-executor"))
            .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
//            .dockerInDockerImages(new String[]{"logstash-executor"})
            .build();

    @ClassRule
    public static MesosCluster cluster = new MesosCluster(config);

    public static Scheduler scheduler;

    ExecutorMessageListenerTestImpl executorMessageListener;

    @BeforeClass
    public static void startScheduler() {

        DockerClient dockerClient = config.dockerClient;

        // TODO move out into a Rule (should belong to mini-mesos)
        // Make our framework executor available inside the mesos cluster
        String[] dindImages = {"mesos/logstash-executor"};
        pushDindImagesToPrivateRegistry(dockerClient, dindImages, config);
        pullDindImagesAndRetagWithoutRepoAndLatestTag(dockerClient, cluster.getMesosContainer().getMesosContainerID(), dindImages);

        scheduler = new Scheduler(cluster.getMesosContainer().getMesosMasterURL(), "mesos/logstash-executor");
        Thread t = new Thread(scheduler::run);

        t.setName("Mesos-Logstash-Scheduler");
        t.setDaemon(true);
        t.start();

        // TODO move out into a Rule
        waitForLogstashFramework();
        waitForExcutorTaskIsRunning();

    }

    @AfterClass
    public static void stopScheduler() {
        // TODO
    }

    private static void waitForLogstashFramework() {
        // wait for our framework
        cluster.waitForState(new Predicate<State>() {
            @Override
            public boolean test(State state) {
                return state.getFramework("logstash") != null;
            }
        });
    }

    private static void waitForExcutorTaskIsRunning() {
        // wait for our executor
        cluster.waitForState(new Predicate<State>() {
            @Override
            public boolean test(State state) {
                return state.getFramework("logstash") != null
                        && state.getFramework("logstash").getTasks().size() > 0
                        && "TASK_RUNNING" .equals(state.getFramework("logstash").getTasks().get(0).getState());
            }
        });
    }

    private static void pullDindImagesAndRetagWithoutRepoAndLatestTag(DockerClient dockerClient, String mesosClusterContainerId, String[] dindImages) {

        for (String image : dindImages) {

            try {
                Thread.sleep(2000); // we have to wait
            } catch (InterruptedException e) {
            }

            ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(mesosClusterContainerId)
                    .withAttachStdout(true).withCmd("docker", "pull", "private-registry:5000/" + image + ":systemtest").exec();
            InputStream execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
            assertThat(DockerUtil.consumeInputStream(execCmdStream), containsString("Download complete"));

            execCreateCmdResponse = dockerClient.execCreateCmd(mesosClusterContainerId)
                    .withAttachStdout(true).withCmd("docker", "tag", "private-registry:5000/" + image + ":systemtest", image + ":latest").exec();

            execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
            DockerUtil.consumeInputStream(execCmdStream);
        }
    }

    private static void pushDindImagesToPrivateRegistry(DockerClient dockerClient, String[] dindImages, MesosClusterConfig config) {
        for (String image : dindImages) {
            String imageWithPrivateRepoName = "localhost:" + config.privateRegistryPort + "/" + image;
            dockerClient.tagImageCmd(image, imageWithPrivateRepoName, "systemtest").withForce(true).exec();
            InputStream responsePushImage = dockerClient.pushImageCmd(imageWithPrivateRepoName).withTag("systemtest").exec();
            assertThat(DockerUtil.consumeInputStream(responsePushImage), containsString("The push refers to a repository"));
        }
    }

    /**
     * We assume that the messages already received are already processed and we can clear the messages list before
     * we query the internal state. Further we assume that there is only one response/message from each executor.
     *
     *
     * @return Messages
     */
    public List<LogstashProtos.ExecutorMessage> requestInternalStatusAndWaitForResponse() {
        int seconds = 10;
        int numberOfExpectedMessages = config.numberOfSlaves;
        executorMessageListener.clearAllMessages();
        scheduler.requestInternalStatus();

        await("Waiting for " + numberOfExpectedMessages + " internal status report messages from executor").atMost(seconds, TimeUnit.SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    return executorMessageListener.getExecutorMessages().size() >= numberOfExpectedMessages;
                } catch (InternalServerErrorException e) {
                    // This probably means that the mesos cluster isn't ready yet..
                    return false;
                }
            }
        });
        return new ArrayList<>(executorMessageListener.getExecutorMessages());
    }
}
