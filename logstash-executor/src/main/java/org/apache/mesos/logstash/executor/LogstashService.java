package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.ConcurrentUtils;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorStatus;
import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.apache.mesos.logstash.executor.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
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
    private final ContainerizerClient client;

    private ExecutorStatus status;

    private final ScheduledExecutorService executorService;
    private final Object lock = new Object();
    private String latestConfig;
    private Process process;

    public LogstashService(ContainerizerClient client) {
        this.client = client;
        status = ExecutorStatus.INITIALIZING;
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        executorService.scheduleWithFixedDelay(this::run, 0, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        ConcurrentUtils.stop(executorService);
    }

    public void update(List<FrameworkInfo> dockerInfo, List<FrameworkInfo> hostInfo) {

        synchronized (lock) {
            latestConfig = ConfigUtil.generateConfigFile(client, dockerInfo, hostInfo);
        }
    }

    private void run() {

        if (process != null) {
            status = (process.isAlive()) ? ExecutorStatus.RUNNING : ExecutorStatus.ERROR;
        }

        String config;
        synchronized (lock) {
            if (latestConfig == null) {
                return;
            }
            config = latestConfig;
            latestConfig = null;
        }

        LOGGER.info("Restarting the Logstash Process.");
        status = ExecutorStatus.RESTARTING;

        try {
            writeConfig("logstash.conf", config);
            process = Runtime.getRuntime().exec("bash /tmp/run_logstash.sh");
        } catch (IOException | ExecutorException e) {
            status = ExecutorStatus.ERROR;
            LOGGER.error("Failed to start logstash process.", e);
        }
    }

    private void writeConfig(String fileName, String content) throws IOException {
        assert !fileName.contains("/"); // should just be the filename, no path

        LOGGER.debug("Writing file: {} with content: {}", fileName, content);

        Path rootDir = Paths.get("/tmp/logstash");

        // Ensure config dir exists.
        Files.createDirectories(rootDir);

        // Write the config
        Path fullFileName = rootDir.resolve(fileName);
        try {
            PrintWriter printWriter = new PrintWriter(fullFileName.toString(), "UTF-8");
            printWriter.write(content);

            printWriter.close();
        } catch (IOException e) {
            LOGGER.error("Error creating. file={}", fullFileName, e);
        }
    }

    public ExecutorStatus status() {
        return status;
    }
}
