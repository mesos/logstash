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

    public static void main(String[] args) {
        new Application().run();
    }

    public void run() {
        LOGGER.info("Starting the executor..");

        MesosExecutorDriver driver = new MesosExecutorDriver(new LogstashExecutor(new TaskStatus()));
        LOGGER.info("Mesos Logstash Executor Started");
        Protos.Status status = driver.run();
        LOGGER.info("Mesos Logstash Executor Stopped");

        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
