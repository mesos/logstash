package org.apache.mesos.logstash.executor;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.logging.FileLogSteamWriter;
import org.apache.mesos.logstash.executor.state.LiveState;

import java.util.logging.Logger;

/**
 * Main application to start the Logstash executor.
 */
public class Application implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Application.class.toString());
    private static final long MAX_LOG_SIZE = 5_000_000;

    public static void main(String[] args) {
        new Application().run();
    }

    public void run() {
        DockerClient dockerClient = new DockerClient();

        LogstashService logstashService = new LogstashService();

        LiveState liveState = new LiveState(logstashService, dockerClient);

        LogstashExecutor executor = new LogstashExecutor(logstashService, dockerClient, liveState);

        MesosExecutorDriver driver = new MesosExecutorDriver(executor);

        // we start after the config manager is initiated
        // because it's sets a frameworkListener
        dockerClient.start();

        LOGGER.info("Mesos Logstash Executor Started");
        Protos.Status status = driver.run();
        LOGGER.info("Mesos Logstash Executor Stopped");

        logstashService.stop();
        dockerClient.stop();

        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
