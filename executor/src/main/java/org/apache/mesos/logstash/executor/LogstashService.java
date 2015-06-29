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


    public LogstashService(Template configTemplate) {
        this.configTemplate = configTemplate;
    }

    public void start() {
        reconfigure(new ArrayList<Framework>());
        try {
            Runtime.getRuntime().exec("bash /tmp/run_logstash.sh").waitFor();
            System.out.println("Logstash service stopped!");
            LOGGER.error("LOGSTASH DOWN");
        }
        catch(IOException | InterruptedException e) {
            LOGGER.error("Something went horribly, horribly wrong: " + e.toString());
        }
    }

    public void reconfigure(List<Framework> knownFrameworks) {
        Map m = new HashMap<>();
        m.put("frameworks", knownFrameworks);

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

    public class LogInputConfiguration {
        LogInputConfiguration(String containerId, String logType, String localLocation) {
            this.containerId = containerId;
            this.logType = logType;
            this.localLogLocation = localLocation;
        }

        String containerId;
        String logType;
        String localLogLocation;
    }
}
