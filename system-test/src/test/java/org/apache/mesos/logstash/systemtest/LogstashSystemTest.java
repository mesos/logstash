package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.InternalServerErrorException;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ContainerState;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType;
import org.apache.mesos.mini.state.State;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.stream.Collectors.toSet;
import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.NOT_STREAMING;
import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.STREAMING;
import static org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorMessageType.STATS;
import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType.DOCKER;
import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType.HOST;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Ignore("Take out individual tests and put them in compact, separate tests. See DeploymentSystemTest.")
public class LogstashSystemTest extends AbstractLogstashFrameworkTest {

    public static final String SOME_LOGSTASH_OUTPUT_FILE = "/tmp/logstash.out";
    public static final String SOME_LOG_FILE = "/tmp/systemtest.log";
    public static final String SOME_OTHER_LOG_FILE = "/tmp/systemtest2.log";

    @Before
    public void addExecutorMessageListener() {
        try {
            Thread.sleep(3_000); // TODO remove and wait if necessary until a clean test setup (e.g. only one running executor) is available...
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorMessageListener = new ExecutorMessageListenerTestImpl();
    }

    @Test
    public void logstashDiscoversOtherRunningContainers() throws Exception {
        requestInternalStatusAndWaitForResponse(
                executorMessages -> 1 == executorMessages.size()
                        && 2 == executorMessages.get(0).getContainersCount());

        requestInternalStatusAndWaitForResponse(
            executorMessages -> 1 == executorMessages.size()
                && 3 == executorMessages.get(0).getContainersCount());
    }

    @Test
    public void logstashHostLogging() throws Exception {
        final String logString = "Hello Test";

        setConfigFor(HOST, "host", getFile("host.full.conf"));

        new HostUtil(cluster.getMesosContainer().getContainerId(), cluster.getConfig().dockerClient)
                .createFileWithContent("/tmp/testhost.log", logString);

        verifyLogstashProcessesLogEvents(SOME_LOGSTASH_OUTPUT_FILE, logString);
    }

    @Test
    public void logstashSetsUpLoggingForFrameworksStartedAfterConfigIsWritten() throws Exception {
        final String logString = "Hello Test";

        setConfigFor(DOCKER, "busybox:latest", getBusyboxConfigFor(SOME_LOG_FILE));
        setConfigFor(HOST, "host", getFile("host.outputonly.conf"));

        verifyLogstashProcessesLogEvents(SOME_LOGSTASH_OUTPUT_FILE, logString);
        verifyExecutorReportsOneStreamingContainer();
    }

    @Test
    public void logstashSetsUpLoggingForFrameworksStartedBeforeConfigIsWritten() throws Exception {
        final String logString = "Hello Test";

        setConfigFor(DOCKER, "busybox:latest", getBusyboxConfigFor(SOME_LOG_FILE));
        setConfigFor(HOST, "host", getFile("host.outputonly.conf"));

        verifyLogstashProcessesLogEvents(SOME_LOGSTASH_OUTPUT_FILE, logString);
        verifyExecutorReportsOneStreamingContainer();
    }

    @Test
    public void logstashReconfiguresLoggingAndStopsObsoleteStreamsAndStartsStreamingOfNewFiles()
        throws Exception {
        final String logStringForLogfile1 = "Hello Test";
        final String logStringForLogfile2 = "Good to see you";

        setConfigFor(DOCKER, "busybox:latest", getBusyboxConfigFor(SOME_LOG_FILE));
        setConfigFor(HOST, "host", getFile("host.outputonly.conf"));

//        startContainer(dummyFramework);
//        dummyFramework.createFileWithContent(SOME_LOG_FILE, logStringForLogfile1);
//        dummyFramework.createFileWithContent(SOME_OTHER_LOG_FILE, logStringForLogfile2);

        verifyLogstashProcessesLogEvents(SOME_LOGSTASH_OUTPUT_FILE, logStringForLogfile1);
//        waitForPsAux(dummyFramework, new String[]{"tail -F " + SOME_LOG_FILE},
//            new String[]{"tail -F " + SOME_OTHER_LOG_FILE});

        // ------- now reconfigure ----------

        setConfigFor(DOCKER, "busybox:latest", getBusyboxConfigFor(SOME_OTHER_LOG_FILE));

        verifyLogstashProcessesLogEvents(SOME_LOGSTASH_OUTPUT_FILE, logStringForLogfile2);
//        waitForPsAux(dummyFramework, new String[]{"tail -F " + SOME_OTHER_LOG_FILE},
//            new String[]{"tail -F " + SOME_LOG_FILE});
    }

    @Test
    public void logstashLoggingForFrameworksWhichIsStartedAndRestartedExterally() throws Exception {
        final String logString = "Hello Test";
        setConfigFor(DOCKER, "busybox:latest", getBusyboxConfigFor(SOME_LOG_FILE));
        setConfigFor(HOST, "host", getFile("host.outputonly.conf"));
//
//        try {
//            dummyFramework.start();
//            waitForNumberOfObservingFrameworks(2); // the executor itself is running in a container too
//
//        } finally {
//            dummyFramework.remove(); // we simulate an external removal of the framework by e.g. marathon scale down
//        }

        waitForNumberOfObservingFrameworks(1);

//        startContainer(dummyFramework); // we start the framework again - this time safely so it's removed automatically
//        dummyFramework.createFileWithContent(SOME_LOG_FILE, logString);

        verifyLogstashProcessesLogEvents(SOME_LOGSTASH_OUTPUT_FILE, logString);
        verifyExecutorReportsOneStreamingContainer();

    }

    private void verifyLogstashProcessesLogEvents(String path, String logString) {
        waitForLogstashToProcessLogEvents(path, logString);
    }

    private void verifyExecutorReportsOneStreamingContainer() {
        List<ExecutorMessage> executorMessages = requestInternalStatusAndWaitForResponse();
        assertEquals(1, executorMessages.size());
        assertEquals(STATS, executorMessages.get(0).getType());

        List<ContainerState> containers = executorMessages.get(0).getContainersList();
        assertEquals(2, containers.size());

        Set<ContainerState.LoggingStateType> stateTypes = containers.stream()
            .map(ContainerState::getType).collect(
                toSet());

        assertThat(stateTypes, containsInAnyOrder(STREAMING, NOT_STREAMING));
    }

    private void waitForLogstashToProcessLogEvents(String path, String logString) {
        try {
            await().atMost(90, TimeUnit.SECONDS).until(() -> {
                try {
                    String logstashOut = executorContainer.getContentOfFile(path);
                    if (logstashOut != null && logstashOut.contains(logString)) {
                        System.out.println("Logstash output: " + logstashOut);
                        return true;
                    }
                    return false;

                } catch (InternalServerErrorException e) {
                    System.out.println(
                        "ERROR while polling logstash executor: " + e);

                    return false;
                }
            });
        } catch (ConditionTimeoutException e) {
            String logstashOut = executorContainer.getContentOfFile(path);
            System.out.println(
                "Unmatched logstash output of executor: " + logstashOut);

            throw e;
        }
    }

    private void waitForNumberOfObservingFrameworks(int count) {
            await().atMost(90, TimeUnit.SECONDS).until(() -> {
                try {
                    List<ExecutorMessage> executorMessages = requestInternalStatusAndWaitForResponse();
                    if (executorMessages.size() == 1 && STATS.equals(executorMessages.get(0).getType())){
                        List<ContainerState> containers = executorMessages.get(0).getContainersList();
                        if (count == containers.size()){
                          return true;
                        }
                    }

                    return false;

                } catch (InternalServerErrorException e) {
                    System.out.println(
                        "ERROR while waiting for " + count + " running containers: " + e );
                    return false;
                }
            });
    }

    private void waitForPsAux(DummyFrameworkContainer framework, String[] existingProcesses,
        String[] notExistingProcesses) {

        try {
            await().atMost(90, TimeUnit.SECONDS).until(() -> {
                try {
                    String psAuxOutput = framework.getPsAuxOutput();

                    if (existingProcesses != null) {
                        for (String process : existingProcesses) {
                            if (!psAuxOutput.contains(process)) {
                                return false;
                            }
                        }
                    }

                    if (notExistingProcesses != null) {
                        for (String process : notExistingProcesses) {
                            if (psAuxOutput.contains(process)) {
                                return false;
                            }
                        }
                    }

                    return true;

                } catch (InternalServerErrorException e) {
                    System.out.println(
                        "ERROR while polling ps aux (" + framework + "): " + e);

                    return false;
                }
            });
        } catch (ConditionTimeoutException e) {
            System.out.println(
                "Unmatched ps aux output of executor (" + framework + "): " + framework
                    .getPsAuxOutput());

            throw e;
        }
    }

    private String getFile(String filename) throws IOException, URISyntaxException {
        Path conf = Paths.get(getClass().getClassLoader().getResource(filename).toURI());
        return new String(Files.readAllBytes(conf));
    }

    private String getBusyboxConfigFor(String file) throws IOException, URISyntaxException {
        return getFile("busybox.conf").replace("{{FILENAME}}", file);
    }

    private void setConfigFor(LogstashConfigType type, String name, String configText) {
        try {
            configManager.save(LogstashProtos.LogstashConfig.newBuilder()
                .setFrameworkName(name)
                .setConfig(configText)
                .setType(type)
                .build());
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
