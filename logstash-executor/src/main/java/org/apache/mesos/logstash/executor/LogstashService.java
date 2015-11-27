package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.ConcurrentUtils;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    public void update(LogstashProtos.LogstashConfiguration logstashConfiguration) {
        // Producer: We only keep the latest config in case of multiple
        // updates.

        List<LS.Plugin> inputPlugins = optionalValuesToList(
                Optional.ofNullable(logstashConfiguration.getLogstashPluginInputSyslog()).map(config -> LS.plugin("syslog", LS.map(LS.kv("port", LS.number(config.getPort()))))),
                Optional.ofNullable(logstashConfiguration.getLogstashPluginInputCollectd()).map(config -> LS.plugin("udp", LS.map(LS.kv("port", LS.number(config.getPort())), LS.kv("buffer_size", LS.number(1452)), LS.kv("codec", LS.plugin("collectd", LS.map())))))
        );

        List<LS.Plugin> outputPlugins = optionalValuesToList(
                Optional.ofNullable(logstashConfiguration.getLogstashPluginOutputElasticsearch()).map(config -> LS.plugin(
                        "elasticsearch",
                        LS.map(
                                LS.kv("host", LS.string(config.getHost())),
                                LS.kv("protocol", LS.string("http")),
                                LS.kv("index", LS.string("logstash"))  //FIXME this should be configurable. Maybe add -%{+YYYY.MM.dd}
                        )
                ))
        );


        String config =
                LS.config(
                        LS.section("input",  inputPlugins.toArray(new LS.Plugin[0])),
                        LS.section("output", outputPlugins.toArray(new LS.Plugin[0]))
                ).serialize();

        LOGGER.debug("Writing new configuration:\n{}", config);
        synchronized (lock) {
            latestConfig = config;
        }
    }

    public static <T> List<T> optionalValuesToList(Optional<T> ... optionals) {
        return Arrays.stream(optionals).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
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
            // Stop any existing logstash instance. It does not have to complete
            // before we start the new one.

            if (process != null) {
                process.destroy();
                process.waitFor(5, TimeUnit.MINUTES);
            }

            process = Runtime.getRuntime().exec(
                    new String[]{
                            "/opt/logstash/bin/logstash",
                            "--log", "/var/log/logstash.log",
                            "-e", newConfig
                    },
                    new String[]{
                            "LS_HEAP_SIZE=" + System.getProperty("mesos.logstash.logstash.heap.size"),
                            "HOME=/root"
                    }
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
