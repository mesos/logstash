package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogStreamManager;
import org.apache.mesos.logstash.executor.docker.DockerStreamer;
import org.apache.mesos.logstash.executor.logging.FileLogSteamWriter;
import org.apache.mesos.logstash.executor.state.LiveState;


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

        FileLogSteamWriter writer = new FileLogSteamWriter(MAX_LOG_SIZE);
        DockerStreamer streamer = new DockerStreamer(writer, dockerClient);
        DockerLogStreamManager streamManager = new DockerLogStreamManager(streamer);

        LogstashService logstashService = new LogstashService(dockerClient);
        logstashService.start();

        ConfigManager configManager = new ConfigManager(logstashService, dockerClient, streamManager);
        LiveState liveState = new LiveState(logstashService, dockerClient, streamManager);

        LogstashExecutor executor = new LogstashExecutor(configManager, dockerClient, liveState);

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
