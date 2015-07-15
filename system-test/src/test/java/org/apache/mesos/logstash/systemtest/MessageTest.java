package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.InternalServerErrorException;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.commons.io.FileUtils;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.state.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;


public class MessageTest extends AbstractLogstashFrameworkTest{

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
    public void addExecutorMessageListener() {
        executorMessageListener = new ExecutorMessageListenerTestImpl();
        scheduler.addExecutorMessageListener(executorMessageListener);
    }

    @After
    public void removeExecutorMessageListener() {
        scheduler.removeAllExecutorMessageListeners();
    }


    @Test
    public void logstashTaskIsRunning() throws Exception {

        State state = cluster.getStateInfo();

        assertEquals("logstash framework should run 1 task", 1, state.getFramework("logstash").getTasks().size());
        assertEquals("LOGSTASH_SERVER", state.getFramework("logstash").getTasks().get(0).getName());
        assertEquals("TASK_RUNNING", state.getFramework("logstash").getTasks().get(0).getState());
    }

    @Test
    public void logstashDiscoversOtherRunningContainers() throws Exception {

        State state = cluster.getStateInfo();

        createAndStartDummyContainer();

        List<ExecutorMessage> executorMessages = requestInternalStatusAndWaitForResponse();
        LogstashProtos.GlobalStateInfo globalStateInfo = executorMessages.get(0).getGlobalStateInfo();
        assertEquals(2, globalStateInfo.getRunningContainerCount()); // the executor itself is running in a container

    }


    @Test
    public void logstashSetsUpLoggingForFrameworksStartedAfterConfigIsWritten() throws Exception {
        final String logString = "Hello Test";
        // create clusterConfig
        FileUtils.write(Paths.get(configFolder.dockerConfDir.getAbsolutePath(), "busybox.conf").toFile(), BUSYBOX_CONF);
        FileUtils.write(Paths.get(configFolder.hostConfDir.getAbsolutePath(), "host.conf").toFile(), HOST_CONF);


        createAndStartDummyContainer();
        simulateLogEvent(logString);

        verifyLogstashProcessesLogEvents(logString);
    }

    @Test
    public void logstashSetsUpLoggingForFrameworksStartedBeforeConfigIsWritten() throws Exception {
        final String logString = "Hello Test";
        // create clusterConfig

        createAndStartDummyContainer();
        simulateLogEvent(logString);

        FileUtils.write(Paths.get(configFolder.dockerConfDir.getAbsolutePath(), "busybox.conf").toFile(), BUSYBOX_CONF);
        FileUtils.write(Paths.get(configFolder.hostConfDir.getAbsolutePath(), "host.conf").toFile(), HOST_CONF);

        verifyLogstashProcessesLogEvents(logString);
    }


    private void verifyLogstashProcessesLogEvents(String logString) {
        waitForLogstashToProcessLogEvents(logString, getExecutorContainerId());


        List<ExecutorMessage> executorMessages = requestInternalStatusAndWaitForResponse();
        assertEquals(1, executorMessages.size());
        assertEquals("GlobalStateInfo", executorMessages.get(0).getType());

        LogstashProtos.GlobalStateInfo globalStateInfo = executorMessages.get(0).getGlobalStateInfo();
        assertEquals(1, globalStateInfo.getConfiguredDockerFrameworkCount());
        assertEquals(1, globalStateInfo.getProcessedContainerCount());
    }


    private void waitForLogstashToProcessLogEvents( final String logString, String executorId) {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;

        execCreateCmdResponse = dockerClient.execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
                .withAttachStdout(true)
                .withCmd("bash", "-c", "docker exec " + executorId + " cat /tmp/logstash.out").exec();


        final ExecCreateCmdResponse finalExecCreateCmdResponse = execCreateCmdResponse;

        await().atMost(90, TimeUnit.SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    InputStream execCmdStream = dockerClient.execStartCmd(finalExecCreateCmdResponse.getId()).exec();
                    String logstashOut = DockerUtil.consumeInputStream(execCmdStream);
                    if (logstashOut != null && logstashOut.contains(logString)) {
                        System.out.println("Logstash output: " + logstashOut);
                        return true;
                    }
                    System.out.println("Unmatched logstash output: " + logstashOut);
                    return false;

                } catch (InternalServerErrorException e) {
                    System.out.println("ERROR while polling logstash executor: " + e);

                    return false;
                }
            }
        });
    }

    private String getExecutorContainerId() {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;


        execCreateCmdResponse = dockerClient.execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
                .withAttachStdout(true)
                .withCmd("bash", "-c", "docker ps | grep logstash | awk '{ print $1 }'").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        return DockerUtil.consumeInputStream(execCmdStream).replaceAll("[^a-z0-9]*","");
    }

    private void simulateLogEvent(String logString) {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient.execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
                .withAttachStdout(true)
                .withCmd("bash", "-c", "echo \"" + logString + "\" > /tmp/systemtest.log").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        System.out.println(DockerUtil.consumeInputStream(execCmdStream));
    }

    private String createAndStartDummyContainer() {
        DockerClient dockerClient = clusterConfig.dockerClient;

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
                .withAttachStdout(true)
                .withCmd("docker", "run", "-td", "-v", "/tmp:/tmp/testlogs", "busybox", "sh").exec();


        InputStream execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        String containerId = DockerUtil.consumeInputStream(execCmdStream).replaceAll("[^a-z0-9]*", "");
        System.out.println(containerId);

        // TODO MAKE HELPER METHOD FOR THIS
        this.containersToBeStopped.add(containerId);

        return containerId;
    }


}
