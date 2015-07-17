package org.apache.mesos.logstash.executor;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.docker.DockerLogSteamManager;
import org.apache.mesos.logstash.executor.docker.DockerStreamer;
import org.apache.mesos.logstash.executor.logging.FileLogSteamWriter;
import org.apache.mesos.logstash.executor.state.DockerInfoCache;
import org.apache.mesos.logstash.executor.state.GlobalStateInfo;

import java.util.logging.Logger;

public class Application implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Application.class.toString());
    private static final long MAX_LOG_SIZE = 5_000_000;

    public static void main(String[] args) {
        new Application().run();
    }

    public void run() {
        DockerClient dockerClient = new DockerClient();
        DockerLogSteamManager streamManager = new DockerLogSteamManager(new DockerStreamer(new FileLogSteamWriter(MAX_LOG_SIZE), dockerClient));
        DockerInfoCache dockerInfoCache = new DockerInfoCache();

        ConfigManager controller = createController(dockerClient, streamManager,dockerInfoCache);
        GlobalStateInfo globalStateInfo = new GlobalStateInfo(dockerClient, streamManager, dockerInfoCache);

        LogstashExecutor executor = new LogstashExecutor(controller, dockerClient, globalStateInfo);
        MesosExecutorDriver driver = new MesosExecutorDriver(executor);

        dockerClient.startMonitoringContainerState(); // we start after the controller is initiated because it's sets a frameworkListener

        LOGGER.info("Mesos Logstash Executor Started");
        Protos.Status status = driver.run();
        LOGGER.info("Mesos Logstash Executor Stopped");

        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    private ConfigManager createController(DockerClient dockerClient, DockerLogSteamManager streamManager, DockerInfoCache dockerInfoCache) {
        LogstashService logstashService = new LogstashService();

        return new ConfigManager(dockerClient, logstashService, streamManager, dockerInfoCache);
    }
}
