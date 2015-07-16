package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.InternalServerErrorException;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.scheduler.ConfigManager;
import org.apache.mesos.logstash.scheduler.ConfigMonitor;
import org.apache.mesos.logstash.scheduler.MesosDriver;
import org.apache.mesos.logstash.scheduler.Scheduler;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.apache.mesos.mini.state.State;
import org.apache.mesos.mini.util.Predicate;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
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


    public static Scheduler scheduler;
    public static ConfigFolder configFolder;

    private static TemporaryFolder folder ;
    protected List<String> containersToBeStopped = new ArrayList<>();

    ExecutorMessageListenerTestImpl executorMessageListener;
    private MesosDriver driver;

    @BeforeClass
    public static void publishExecutorInMesosCluster() throws IOException {

        DockerClient dockerClient = clusterConfig.dockerClient;


        // TODO move out into a Rule (should belong to mini-mesos)
        // Make our framework executor available inside the mesos cluster
        String[] dindImages = {"mesos/logstash-executor"};
        pushDindImagesToPrivateRegistry(dockerClient, dindImages, clusterConfig);
        pullDindImagesAndRetagWithoutRepoAndLatestTag(dockerClient, cluster.getMesosContainer().getMesosContainerID(), dindImages);
    }

    @After
    public void stopContainers() {

        if (containersToBeStopped.isEmpty()){
            return;
        }

        String stopString = "docker stop -t=0 " + containersToBeStopped.stream().collect(Collectors.joining(" "));

        System.out.println("STOPPING test containers: " + stopString);

        String execId = clusterConfig.dockerClient.execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
                .withCmd("bash", "-c", stopString).exec().getId();

        InputStream inputStream = clusterConfig.dockerClient.execStartCmd(execId).exec();
        String output = DockerUtil.consumeInputStream(inputStream);
        System.out.println(output);
    }

    @Before
    public void startLogstashFramework() throws IOException {
        folder = new TemporaryFolder();
        folder.create();

        File dockerConf  = folder.newFolder("docker");
        File hostConf  = folder.newFolder("host");

        configFolder = new ConfigFolder(dockerConf, hostConf);

        driver = new MesosDriver(cluster.getMesosContainer().getMesosMasterURL());

        ConfigManager configManager = new ConfigManager(
                new ConfigMonitor(dockerConf.getAbsolutePath()),
                new ConfigMonitor(hostConf.getAbsolutePath())
        );

        scheduler = new Scheduler(driver, configManager);

        scheduler.start();
        configManager.start();

        Thread t = new Thread(() -> driver.run(scheduler));

        System.out.println("**************** RUNNING CONTAINERS ON TEST START *******************");
        printRunningContainers();
        System.out.println("*********************************************************************");

        t.setName("Mesos-Logstash-Scheduler");
        t.setDaemon(true);
        t.start();

        // TODO move out into a Rule
        waitForLogstashFramework();
        waitForExcutorTaskIsRunning();
    }


    private String printRunningContainers() {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient.execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
                .withAttachStdout(true)
                .withCmd("bash", "-c", "docker ps").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        String runningDockerContainers = DockerUtil.consumeInputStream(execCmdStream);

        System.out.println(runningDockerContainers);

        return runningDockerContainers;
    }


    @After
    public void stopLogstashFramework(){
        driver.stop();
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
    public List<ExecutorMessage> requestInternalStatusAndWaitForResponse(Predicate<List<ExecutorMessage>> predicate) {
        int seconds = 10;
        int numberOfExpectedMessages = clusterConfig.numberOfSlaves;
        executorMessageListener.clearAllMessages();
        scheduler.requestInternalStatus();

        await("Waiting for " + numberOfExpectedMessages + " internal status report messages from executor").atMost(seconds, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    if (executorMessageListener.getExecutorMessages().size() >= numberOfExpectedMessages) {

                        if (predicate.test(executorMessageListener.getExecutorMessages())) {
                            return true;
                        } else {
                            executorMessageListener.clearAllMessages();
                            scheduler.requestInternalStatus();
                            return false;
                        }
                    }
                    return false;
                } catch (InternalServerErrorException e) {
                    // This probably means that the mesos cluster isn't ready yet..
                    return false;
                }
            }
        });
        return new ArrayList<>(executorMessageListener.getExecutorMessages());
    }

    /**
     * We assume that the messages already received are already processed and we can clear the messages list before
     * we query the internal state. Further we assume that there is only one response/message from each executor.
     *
     *
     * @return Messages
     */
    public List<ExecutorMessage> requestInternalStatusAndWaitForResponse() {
        return  requestInternalStatusAndWaitForResponse(new Predicate<List<ExecutorMessage>>() {
            @Override
            public boolean test(List<ExecutorMessage> executorMessages) {
                return true;
            }
        });
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
