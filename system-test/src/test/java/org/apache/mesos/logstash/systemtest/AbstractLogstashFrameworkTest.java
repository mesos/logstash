package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.InternalServerErrorException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.scheduler.Application;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public abstract class AbstractLogstashFrameworkTest {

    private static final String DOCKER_PORT = "2376";

    @ClassRule
    protected static final MesosCluster cluster = new MesosCluster(MesosClusterConfig.builder()
                            .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
                            .build());

    public static DockerClient clusterDockerClient;

    ExecutorMessageListenerTestImpl executorMessageListener;
    protected ConfigManager configManager;
    public LogstashExecutorContainer executorContainer;

    @BeforeClass
    public static void publishExecutorInMesosCluster() throws IOException {
        DockerClientConfig.DockerClientConfigBuilder dockerConfigBuilder = DockerClientConfig
                .createDefaultConfigBuilder()
                .withUri("http://" + cluster.getMesosMasterContainer().getIpAddress() + ":" + DOCKER_PORT);
        clusterDockerClient = DockerClientBuilder.getInstance(dockerConfigBuilder.build()).build();

        LogstashSchedulerContainer schedulerContainer = new LogstashSchedulerContainer(clusterDockerClient, cluster.getMesosMasterContainer().getIpAddress());
        schedulerContainer.start();
    }

    @Before
    public void startLogstashFramework() throws IOException, ExecutionException, InterruptedException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();

        Configuration configuration = new Application().getLogstashConfiguration();
        configuration.setDisableFailover(true); // we remove our framework completely
        configuration.setVolumeString("/tmp");

        configManager = new ConfigManager(configuration);
        configManager.start();

        System.out.println("**************** RECONCILIATION_DONE CONTAINERS ON TEST START *******************");
        printRunningContainers();
        System.out.println("*********************************************************************");

        waitForLogstashFramework();
        waitForExcutorTaskIsRunning();

        executorContainer = new LogstashExecutorContainer(clusterDockerClient);
    }

    private void printRunningContainers() {
        DockerClient dockerClient = cluster.getConfig().dockerClient;

        List<Container> containers = dockerClient.listContainersCmd().exec();
        for (Container container : containers) {
            System.out.println(container.getImage());
        }
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
        int numberOfExpectedMessages = cluster.getConfig().slaveResources.length;

        executorMessageListener.clearAllMessages();

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
}
