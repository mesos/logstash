package org.apache.mesos.logstash.executor;


import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashService {

    public static final Logger LOGGER = Logger.getLogger(LogstashService.class.toString());

    private Process logstashProcess = null;

    public boolean hasStarted() {
        return this.logstashProcess != null;
    }

    public String getStaticConfig() {
        return staticConfig;
    }

    public void setStaticConfig(String staticConfig) {
        this.staticConfig = staticConfig;
        // TODO reconfigure!
    }

    // TODO should be empty string by default
    private String staticConfig = "output { file { path => \"/tmp/logstash-test.log\" }}";

    public void start() {
        if(this.logstashProcess == null) {
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

    public void reconfigure(Map<String, DockerFramework> logConfigurations) {

        try {
            PrintWriter printWriter = new PrintWriter("/tmp/logstash.conf", "UTF-8");

            // TODO use a directory to configure logstash and write each Map.Entry as a separate
            // file instead.
            printWriter.write(logConfigurations
                    .entrySet().stream()
                    .map(e -> e.getValue().generateLogstashConfig(e.getKey()))
                    .collect(Collectors.joining("\n")));

            printWriter.write(staticConfig);

            printWriter.close();
        }
        catch(IOException e) {
            LOGGER.error("Error creating logstash.conf", e);
        }
    }
}
