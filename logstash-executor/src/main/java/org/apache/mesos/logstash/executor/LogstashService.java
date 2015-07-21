package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash.
 */
public class LogstashService {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashService.class);

    private String staticConfig;
    private String dockerConfig;

    public void updateConfig(LogType type, String config) {
        LOGGER.debug("New Config for {}:\n{}", type, config);
        switch (type) {
            case HOST:
                updateStaticConfig(config);
                break;
            case DOCKER:
                updateDockerConfig(config);
                break;
        }
    }

    private void updateStaticConfig(String staticConfig) {
        if (staticConfig.equals(this.staticConfig)) {
            LOGGER.info("Static framework configuration did not change");
        } else {
            this.staticConfig = staticConfig;
            assertStarted();
        }
    }

    private void updateDockerConfig(String config) {
        if (config.equals(this.dockerConfig)) {
            LOGGER.info("Docker framework configuration did not change");
        } else {
            this.dockerConfig = config;
            assertStarted();
        }
    }

    private void assertStarted() {

        if (this.dockerConfig == null || this.staticConfig == null) {
            LOGGER.warn("Logstash config files haven't been set up yet. Can't start logstash");
            return;
        }

        LOGGER.info("(Re)starting logstash...");

        try {
            writeConfig("logstash-docker.conf", this.dockerConfig);
            writeConfig("logstash-static.conf", this.staticConfig);
            Runtime.getRuntime().exec("bash /tmp/run_logstash.sh");
        } catch (IOException e) {
            LOGGER.error("Something went horribly, horribly wrong:", e);
        }
    }

    // TODO: Why is this synchronized?
    private synchronized void writeConfig(String fileName, String content) throws IOException {
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
}
