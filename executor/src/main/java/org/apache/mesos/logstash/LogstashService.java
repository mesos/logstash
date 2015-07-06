package org.apache.mesos.logstash;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashService {

    public static final Logger LOGGER = Logger.getLogger(LogstashService.class.toString());

    private Process logstashProcess = null;

    public void updateStaticConfig(String staticConfig) {
        if(staticConfig.equals(this.staticConfig)) {
            LOGGER.info("Static framework configuration did not change");
        } else {
            this.staticConfig = staticConfig;
            assertStarted();
        }
    }

    public void updateDockerConfig(String config) {
        if(config.equals(this.dockerConfig)) {
            LOGGER.info("Docker framework configuration did not change");
        } else {
            this.dockerConfig = config;
            assertStarted();
        }
    }

    private String staticConfig;
    private String dockerConfig;

    private void assertStarted() {
        if(this.dockerConfig == null || this.staticConfig == null) {
            LOGGER.warn("Logstash config files haven't been set up yet. Can't start logstash");
            return;
        }
        LOGGER.info("(Re)starting logstash...");
        try {
            writeDockerConfig();
            writeStaticConfig();
            this.logstashProcess = Runtime.getRuntime().exec("bash /tmp/run_logstash.sh");
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

    private void writeConfig(String fileName, String content) {
        assert fileName.indexOf("/") == -1;// should just be the filename, no path

        // Ensure config dir exists
        Paths.get("/tmp", "logstash").toFile().mkdirs();

        // Write the config
        String fullFileName = Paths.get("/tmp", "logstash", fileName).toString();
        try {
            PrintWriter printWriter = new PrintWriter(fullFileName, "UTF-8");
            printWriter.write(content);

            printWriter.close();
        }
        catch(IOException e) {
            LOGGER.error(String.format("Error creating %s", fullFileName), e);
        }
    }
}
