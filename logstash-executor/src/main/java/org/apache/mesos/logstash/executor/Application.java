package org.apache.mesos.logstash.executor;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application to start the Logstash executor.
 */
public class Application implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        new Application().run();
    }

    public void run() {
        LogstashService logstashService = new LogstashService();

        LogstashExecutor executor = new LogstashExecutor(logstashService);

        MesosExecutorDriver driver = new MesosExecutorDriver(executor);

        // we start after the config manager is initiated
        // because it's sets a frameworkListener
//        dockerClient.start();

        LOGGER.info("Mesos Logstash Executor Started");
        Protos.Status status = driver.run();
        LOGGER.info("Mesos Logstash Executor Stopped");

        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        }
        else {
            System.exit(1);
        }
    }
}
