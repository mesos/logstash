package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.InternalServerErrorException;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.state.State;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.stream.Collectors.toSet;
import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.NOT_STREAMING;
import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.STREAMING;
import static org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorMessageType.STATS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MessageSystemTest extends AbstractLogstashFrameworkTest {

    public static final String BUSYBOX_CONF =
        "input {\n" +
            "  file {\n" +
            "    docker-path => \"/tmp/testlogs/systemtest.log\"\n" +
            "    start_position => \"beginning\"\n" +
            "    debug => true\n" +
            "  }\n" +
            "}\n";
    private static final String HOST_CONF = "output { file {path=>\"/tmp/logstash.out\" \n" +
        "codec => \"plain\" \n" +
        "flush_interval => 0}}"; // the flush interval is important for our test

    @Before
    public void giveTimeToDockerDaemonCleanup() {

        // Sometimes we have lingering 'zombie'-containers that
        // cause tests to fail. Ensure docker has time to stop all
        // containers between every test

        try {
            Thread.sleep(3_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void addExecutorMessageListener() {
        executorMessageListener = new ExecutorMessageListenerTestImpl();
        scheduler.registerListener(executorMessageListener);
    }

    @Test
    public void logstashTaskIsRunning() throws Exception {

        State state = cluster.getStateInfo();

        assertEquals("logstash framework should run 1 task", 1,
            state.getFramework("logstash").getTasks().size());
        assertEquals("logstash.task", state.getFramework("logstash").getTasks().get(0).getName());
        assertEquals("TASK_RUNNING", state.getFramework("logstash").getTasks().get(0).getState());
    }

    @Test
    public void logstashDiscoversOtherRunningContainers() throws Exception {
        createAndStartDummyContainer();
        requestInternalStatusAndWaitForResponse(
            executorMessages -> 1 == executorMessages.size()
                && 2 == executorMessages.get(0).getContainersCount());
    }

    @Test
    public void logstashSetsUpLoggingForFrameworksStartedAfterConfigIsWritten() throws Exception {
        final String logString = "Hello Test";

        Files.write(configFolder.dockerConfDir.toPath().resolve("busybox.conf"), BUSYBOX_CONF.getBytes());
        Files.write(configFolder.hostConfDir.toPath().resolve("host.conf"), HOST_CONF.getBytes());

        createAndStartDummyContainer();
        simulateLogEvent(logString);

        verifyLogstashProcessesLogEvents(logString);
    }

    @Test
    public void logstashSetsUpLoggingForFrameworksStartedBeforeConfigIsWritten() throws Exception {
        final String logString = "Hello Test";

        createAndStartDummyContainer();
        simulateLogEvent(logString);

        Files.write(configFolder.dockerConfDir.toPath().resolve("busybox.conf"),
            BUSYBOX_CONF.getBytes());
        Files.write(configFolder.hostConfDir.toPath().resolve("host.conf"), HOST_CONF.getBytes());

        verifyLogstashProcessesLogEvents(logString);
    }

    private void verifyLogstashProcessesLogEvents(String logString) {
        waitForLogstashToProcessLogEvents(logString, getExecutorContainerId());

        List<ExecutorMessage> executorMessages = requestInternalStatusAndWaitForResponse();
        assertEquals(1, executorMessages.size());
        assertEquals(STATS, executorMessages.get(0).getType());

        List<ContainerState> containers = executorMessages.get(0).getContainersList();
        assertEquals(2, containers.size());

        Set<LoggingStateType> stateTypes = containers.stream()
            .map(ContainerState::getType).collect(
                toSet());

        assertThat(stateTypes, containsInAnyOrder(STREAMING, NOT_STREAMING));
    }

    private void waitForLogstashToProcessLogEvents(final String logString, String executorId) {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("bash", "-c", "docker exec " + executorId + " cat /tmp/logstash.out").exec();

        final ExecCreateCmdResponse finalExecCreateCmdResponse = execCreateCmdResponse;

        try {
            await().atMost(90, TimeUnit.SECONDS).until(() -> {
                try {
                    InputStream execCmdStream = dockerClient
                        .execStartCmd(finalExecCreateCmdResponse.getId()).exec();
                    String logstashOut = DockerUtil.consumeInputStream(execCmdStream);
                    if (logstashOut != null && logstashOut.contains(logString)) {
                        System.out.println("Logstash output: " + logstashOut);
                        return true;
                    }
                    return false;

                } catch (InternalServerErrorException e) {
                    System.out.println(
                        "ERROR while polling logstash executor (" + executorId + "): " + e);

                    return false;
                }
            });
        } catch (ConditionTimeoutException e) {
            InputStream execCmdStream = dockerClient
                .execStartCmd(finalExecCreateCmdResponse.getId()).exec();
            String logstashOut = DockerUtil.consumeInputStream(execCmdStream);
            System.out.println(
                "Unmatched logstash output of executor (" + executorId + "): " + logstashOut);

            throw e;
        }
    }

    private String getExecutorContainerId() {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("bash", "-c", "docker ps | grep logstash | awk '{ print $1 }'").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        return DockerUtil.consumeInputStream(execCmdStream).replaceAll("[^a-z0-9]*", "");
    }

    private void simulateLogEvent(String logString) {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("bash", "-c", "echo \"" + logString + "\" > /tmp/systemtest.log").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        System.out.println(DockerUtil.consumeInputStream(execCmdStream));
    }

    private String createAndStartDummyContainer() {
        DockerClient dockerClient = clusterConfig.dockerClient;

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("docker", "run", "-td", "-v", "/tmp:/tmp/testlogs", "busybox", "sh").exec();

        InputStream execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        String containerId = DockerUtil.consumeInputStream(execCmdStream)
            .replaceAll("[^a-z0-9]*", "");
        System.out.println(containerId);

        // TODO MAKE HELPER METHOD FOR THIS
        this.containersToBeStopped.add(containerId);

        return containerId;
    }
}
