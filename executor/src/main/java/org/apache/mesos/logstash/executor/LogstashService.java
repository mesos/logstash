package org.apache.mesos.logstash.executor;


import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.log4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashService {

    public static final Logger LOGGER = Logger.getLogger(LogstashService.class.toString());

    public static final String LOGSTASH_IMAGE = "logstash";

    private Template configTemplate;

    private Process logstashProcess = null;

    public LogstashService(Template configTemplate) {
        this.configTemplate = configTemplate;
    }

    public boolean hasStarted() {
        return this.logstashProcess != null;
    }

    public void start() {
        if(this.logstashProcess == null) {
            LOGGER.info("Starting logstash...");
            try {
                this.logstashProcess = Runtime.getRuntime().exec("bash /tmp/run_logstash.sh");
            } catch (IOException e) {
                LOGGER.error("Something went horribly, horribly wrong: " + e.toString());
            }
        } else {
            LOGGER.info("Logstash already started...");
        }
    }

    public void reconfigure(Map<String, Framework> logConfigurations) {
        Map m = new HashMap<>();
        m.put("configurations", logConfigurations);

        try {
            FileOutputStream os = new FileOutputStream("/tmp/logstash.conf");
            configTemplate.process(m, new OutputStreamWriter(os));
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(TemplateException e) {
            e.printStackTrace();
        }
    }
}
