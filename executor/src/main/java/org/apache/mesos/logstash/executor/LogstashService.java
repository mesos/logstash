package org.apache.mesos.logstash.executor;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash.
 */
public class LogstashService implements LogstashManager {

    public static final Logger LOGGER = Logger.getLogger(LogstashService.class);

    private String staticConfig;
    private String dockerConfig;

    @Override
    public void updateConfig(LogType type, String config) {
        LOGGER.debug("New Config for " + type + ":\n" + config);
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
            writeDockerConfig();
            writeStaticConfig();
            Runtime.getRuntime().exec("bash /tmp/run_logstash.sh");
        } catch (IOException e) {
            LOGGER.error("Something went horribly, horribly wrong:", e);
        }
    }

    private void writeStaticConfig() {
        writeConfig("logstash-static.conf", this.staticConfig);
    }

    private void writeDockerConfig() {
        writeConfig("logstash-docker.conf", this.dockerConfig);
    }

    private synchronized void writeConfig(String fileName, String content) {
        assert !fileName.contains("/");// should just be the filename, no path

        LOGGER.debug(String.format("Writing file: %s with content: %s", fileName, content));

        // TODO: Make sure this does not fail.
        // Ensure config dir exists
        Paths.get("/tmp", "logstash").toFile().mkdirs();

        // Write the config
        String fullFileName = Paths.get("/tmp", "logstash", fileName).toString();
        try {
            PrintWriter printWriter = new PrintWriter(fullFileName, "UTF-8");
            printWriter.write(content);

            printWriter.close();
        } catch (IOException e) {
            LOGGER.error(String.format("Error creating %s", fullFileName), e);
        }
    }
}
