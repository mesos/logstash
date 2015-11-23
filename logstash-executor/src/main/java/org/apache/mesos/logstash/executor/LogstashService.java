package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.ConcurrentUtils;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorStatus;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash.
 */
public class LogstashService {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashService.class);

    private ExecutorStatus status;

    private final ScheduledExecutorService executorService;
    private final Object lock = new Object();
    private String latestConfig;
    private Process process;

    public LogstashService() {
        status = ExecutorStatus.INITIALIZING;
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        executorService.scheduleWithFixedDelay(this::run, 0, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        if (process != null) {
            process.destroy();
        }
        ConcurrentUtils.stop(executorService);
    }

    public void update(int syslogPort, String elasticsearchDomainAndPort) {
        // Producer: We only keep the latest config in case of multiple
        // updates.

        String config =
                LS.config(
                        LS.section("input",
                            LS.plugin("syslog", LS.map(
                                    LS.kv("port", LS.number(syslogPort))
                            ))
                        ),
                        LS.section("output",
                            LS.plugin("elasticsearch", LS.map(
                                    LS.kv("hosts", LS.array(LS.string(elasticsearchDomainAndPort)))
                            ))
                        )
                ).serialize();

        LOGGER.debug("Writing new configuration:\n{}", config);
        synchronized (lock) {
            latestConfig = config;
        }
    }

    private void run() {

        if (process != null) {
            status = process.isAlive() ? ExecutorStatus.RUNNING : ExecutorStatus.ERROR;
        }

        // Consumer: Read the latest config. If any, write it to disk and restart
        // the logstash process.
        String newConfig = getLatestConfig();

        if (newConfig == null) {
            return;
        }

        LOGGER.info("Restarting the Logstash Process.");
        status = ExecutorStatus.RESTARTING;

        try {
            Path configFile = Paths.get("/tmp/logstash/logstash.conf");
            Files.createDirectories(configFile.getParent());
            Files.write(configFile.resolve(configFile), newConfig.getBytes());

            // Stop any existing logstash instance. It does not have to complete
            // before we start the new one.

            if (process != null) {
                process.destroy();
                process.waitFor(5, TimeUnit.MINUTES);
            }

            process = Runtime.getRuntime().exec("bash /tmp/run_logstash.sh",
                    new String[]{"LS_HEAP_SIZE=" + System.getProperty("mesos.logstash.logstash.heap.size")}
            );
        } catch (Exception e) {
            status = ExecutorStatus.ERROR;
            LOGGER.error("Failed to start logstash process.", e);
        }
    }

    public ExecutorStatus status() {
        return status;
    }

    private String getLatestConfig() {
        synchronized (lock) {
            String config = latestConfig;
            latestConfig = null;
            return config;
        }
    }

}
