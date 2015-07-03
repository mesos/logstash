package org.apache.mesos.logstash;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
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
            writeConfig("logstash-static.conf", this.staticConfig);
            assertStarted();
        }
    }

    public void updateDockerConfig(String config) {
        if(config.equals(this.dockerConfig)) {
            LOGGER.info("Docker framework configuration did not change");
        } else {
            this.dockerConfig = config;
            writeConfig("logstash-docker.conf", this.dockerConfig);
        }
        assertStarted();
    }

    // TODO should be empty string by default
    private String staticConfig = "output { file { path => \"/tmp/logstash-test.log\" }}";
    private String dockerConfig;

    private void assertStarted() {
        if(!hasStarted()) {
            LOGGER.info("Starting logstash...");
            try {
                this.logstashProcess = Runtime.getRuntime().exec("bash /tmp/run_logstash.sh");
            } catch (IOException e) {
                LOGGER.error("Something went horribly, horribly wrong:", e);
            }
        } else {
            LOGGER.info("Logstash already started...");
        }
    }

    private void writeConfig(String fileName, String content) {
        String fullFileName = Paths.get("/tmp", fileName).toString();
        try {
            PrintWriter printWriter = new PrintWriter(fullFileName, "UTF-8");
            printWriter.write(content);

            printWriter.close();
        }
        catch(IOException e) {
            LOGGER.error(String.format("Error creating %s", fullFileName), e);
        }
    }

    private boolean hasStarted() {
        return this.logstashProcess != null;
    }

}
