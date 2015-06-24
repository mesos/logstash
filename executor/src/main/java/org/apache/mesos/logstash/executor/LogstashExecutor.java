package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

import java.lang.InterruptedException;
import java.net.*;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

/**
 * Executor for Logstash.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = Logger.getLogger(LogstashExecutor.class.toString());

    public static void main(String[] args) {
        // Hack to get around ServiceLoader not finding DockerCmdExecFactoryImpl on the class path when executed from mesos
        loadDockerClientToCacheClassLoader(getHostAddress());

        LOGGER.info("Executor running?!");

        LOGGER.info("Started LogstashExecutor");

        MesosExecutorDriver driver = new MesosExecutorDriver(new LogstashExecutor());
        Protos.Status status = driver.run();
        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Logstash registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Logstash re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Logstash disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(status);

        String hostAddress = getHostAddress();
        doIt(hostAddress);

        try {
            Thread.sleep(120_000);
        } catch (InterruptedException e) {
            LOGGER.error("INTERRUPTED");
        }


        try {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setState(Protos.TaskState.TASK_FINISHED).build();
                    driver.sendStatusUpdate(taskStatus);
                }
            }) {
            });
        } catch (Exception e) {
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_FAILED).build();
            driver.sendStatusUpdate(status);
        }
    }

    private static void loadDockerClientToCacheClassLoader(String hostAddress) {
        DockerClientBuilder.getInstance(hostAddress).build();
    }

    private void doIt(String hostAddress) {
        LOGGER.info("Host address is: " + hostAddress);

        DockerClient dockerClient = DockerClientBuilder.getInstance(hostAddress).build();
        List<Container> containers = dockerClient.listContainersCmd().exec();

        LOGGER.info(String.format("Number of containers running %d", containers.size()));

        for (Container c : containers) {
            LOGGER.info(String.format("Container %d has id %s", containers.indexOf(c) + 1, c.getId()));
        }

        com.spotify.docker.client.DockerClient spotifyDockerClient = com.spotify.docker.client.DefaultDockerClient.builder()
                .uri(URI.create(hostAddress))
                .build();

        new LogstashConnector(new DockerInfoImpl(dockerClient, spotifyDockerClient)).init();
    }

    private static String getHostAddress() {
        String hostAddress = null;
        try {
            Enumeration<InetAddress> inetAddresses = NetworkInterface.getByName("eth0").getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress a = inetAddresses.nextElement();
                if (a instanceof Inet6Address) {
                    continue;
                }

                hostAddress = String.format("http:/%s:2376", a.toString());
                LOGGER.info("Host address is: " + hostAddress);
            }

        } catch (SocketException se) {
            se.printStackTrace();
        }

        return hostAddress;
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_FAILED).build();
        driver.sendStatusUpdate(status);
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message: " + Arrays.toString(data));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down framework...");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
    }
}
