package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.InternalServerErrorException;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.logstash.scheduler.LogstashScheduler;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.LogstashLiveState;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.apache.mesos.mini.util.Predicate;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThat;

public abstract class AbstractLogstashFrameworkTest {
    public static final MesosClusterConfig clusterConfig = MesosClusterConfig.builder()
        // Note: Logstash-mesos uses container discovery, and mesos-local runs all
        // the executors in the same docker host. So it is safest to just use 1 slave for now..
        .numberOfSlaves(1).privateRegistryPort(3333).proxyPort(12345)
            //            .imagesToBuild(new MesosClusterConfig.ImageToBuild(new File("../executor"), "logstash-executor"))
        .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
            //            .dockerInDockerImages(new String[]{"logstash-executor"})
        .build();

    @ClassRule
    public static MesosCluster cluster = new MesosCluster(clusterConfig);

    public static LogstashScheduler scheduler;
    public static ConfigFolder configFolder;

    protected List<String> containersToBeStopped = new ArrayList<>();

    ExecutorMessageListenerTestImpl executorMessageListener;

    @BeforeClass
    public static void publishExecutorInMesosCluster() throws IOException {

        DockerClient dockerClient = clusterConfig.dockerClient;

        // TODO move out into a Rule (should belong to mini-mesos)
        // Make our framework executor available inside the mesos cluster
        String[] dindImages = {"mesos/logstash-executor"};
        pushDindImagesToPrivateRegistry(dockerClient, dindImages, clusterConfig);
        pullDindImagesAndRetagWithoutRepoAndLatestTag(dockerClient,
            cluster.getMesosContainer().getMesosContainerID(), dindImages);
    }

    @After
    public void stopContainers() {
        scheduler.stop();

        if (containersToBeStopped.isEmpty()) {
            return;
        }

        String stopString =
            "docker stop -t=0 " + containersToBeStopped.stream().collect(Collectors.joining(" "));

        System.out.println("STOPPING test containers: " + stopString);

        String execId = clusterConfig.dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withCmd("bash", "-c", stopString).exec().getId();

        InputStream inputStream = clusterConfig.dockerClient.execStartCmd(execId).exec();
        String output = DockerUtil.consumeInputStream(inputStream);
        System.out.println(output);
    }

    @Before
    public void startLogstashFramework() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();

        File dockerConf  = folder.newFolder("docker");
        File hostConf  = folder.newFolder("host");

        configFolder = new ConfigFolder(dockerConf, hostConf);

        LiveState liveState = new LogstashLiveState();

        scheduler = new LogstashScheduler(liveState, new LogstashSettings(null, null),
            cluster.getMesosContainer().getMesosMasterURL(), false);
        scheduler.start();

        ConfigManager configManager = new ConfigManager(scheduler, folder.getRoot().toPath());
        configManager.start();

        System.out.println("**************** RUNNING CONTAINERS ON TEST START *******************");
        printRunningContainers();
        System.out.println("*********************************************************************");

        // TODO move out into a Rule
        waitForLogstashFramework();
        waitForExcutorTaskIsRunning();
    }

    private String printRunningContainers() {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("bash", "-c", "docker ps").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        String runningDockerContainers = DockerUtil.consumeInputStream(execCmdStream);

        System.out.println(runningDockerContainers);

        return runningDockerContainers;
    }



    private static void waitForLogstashFramework() {
        // wait for our framework
        cluster.waitForState(state -> state.getFramework("logstash") != null);
    }

    private static void waitForExcutorTaskIsRunning() {
        // wait for our executor
        cluster.waitForState(state -> state.getFramework("logstash") != null
            && state.getFramework("logstash").getTasks().size() > 0
            && "TASK_RUNNING".equals(state.getFramework("logstash").getTasks().get(0).getState()));
    }

    private static void pullDindImagesAndRetagWithoutRepoAndLatestTag(DockerClient dockerClient,
        String mesosClusterContainerId, String[] dindImages) {

        for (String image : dindImages) {

            try {
                Thread.sleep(2000); // we have to wait
            } catch (InterruptedException ignored) {
            }

            ExecCreateCmdResponse execCreateCmdResponse = dockerClient
                .execCreateCmd(mesosClusterContainerId)
                .withAttachStdout(true)
                .withCmd("docker", "pull", "private-registry:5000/" + image + ":systemtest").exec();
            InputStream execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId())
                .exec();
            assertThat(DockerUtil.consumeInputStream(execCmdStream),
                Matchers.containsString("Download complete"));

            execCreateCmdResponse = dockerClient.execCreateCmd(mesosClusterContainerId)
                .withAttachStdout(true)
                .withCmd("docker", "tag", "private-registry:5000/" + image + ":systemtest",
                    image + ":latest").exec();

            execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
            DockerUtil.consumeInputStream(execCmdStream);
        }
    }

    private static void pushDindImagesToPrivateRegistry(DockerClient dockerClient,
        String[] dindImages, MesosClusterConfig config) {
        for (String image : dindImages) {
            String imageWithPrivateRepoName =
                "localhost:" + config.privateRegistryPort + "/" + image;
            dockerClient.tagImageCmd(image, imageWithPrivateRepoName, "systemtest").withForce(true)
                .exec();
            InputStream responsePushImage = dockerClient.pushImageCmd(imageWithPrivateRepoName)
                .withTag("systemtest").exec();
            assertThat(DockerUtil.consumeInputStream(responsePushImage),
                Matchers.containsString("The push refers to a repository"));
        }
    }

    /**
     * We assume that the messages already received are already
     * processed and we can clear the messages list before
     * we query the internal state. Further we assume that there
     * is only one response/message from each executor.
     *
     * @return Messages
     */
    public List<ExecutorMessage> requestInternalStatusAndWaitForResponse(
        Predicate<List<ExecutorMessage>> predicate) {
        int seconds = 10;
        int numberOfExpectedMessages = clusterConfig.numberOfSlaves;

        executorMessageListener.clearAllMessages();
        scheduler.requestExecutorStats();

        String message = String
            .format("Waiting for %d internal status report messages from executor",
                numberOfExpectedMessages);
        await(message).atMost(seconds, SECONDS).pollInterval(1, SECONDS).until(() -> {
            try {
                if (executorMessageListener.getExecutorMessages().size()
                    >= numberOfExpectedMessages) {

                    if (predicate.test(executorMessageListener.getExecutorMessages())) {
                        return true;
                    } else {
                        executorMessageListener.clearAllMessages();
                        scheduler.requestExecutorStats();
                        return false;
                    }
                }
                return false;
            } catch (InternalServerErrorException e) {
                // This probably means that the mesos cluster isn't ready yet..
                return false;
            }
        });

        return new ArrayList<>(executorMessageListener.getExecutorMessages());
    }

    /**
     * We assume that the messages already received are already
     * processed and we can clear the messages list before
     * we query the internal state. Further we assume that there
     * is only one response/message from each executor.
     *
     * @return Messages
     */
    public List<ExecutorMessage> requestInternalStatusAndWaitForResponse() {
        return requestInternalStatusAndWaitForResponse(executorMessages -> true);
    }

    static class ConfigFolder {
        final File dockerConfDir;
        final File hostConfDir;

        ConfigFolder(File dockerConfDir, File hostConfDir) {
            this.dockerConfDir = dockerConfDir;
            this.hostConfDir = hostConfDir;
        }
    }
}
