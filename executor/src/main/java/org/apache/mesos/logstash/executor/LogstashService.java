package org.apache.mesos.logstash.executor;


import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

    private DockerInfo client;
    private String currentContainerId;
    private Template configTemplate;


    public LogstashService(DockerInfo client) {
        this.client = client;

        initTemplatingEngine();
    }

    public void start() {
        if(currentContainerId == null) {
            //String containerId = client.startContainer(LOGSTASH_IMAGE);
        }
    }

    public void stop() {
        if(currentContainerId != null) {
            //client.stopContainer(currentContainerId);
            currentContainerId = null;
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

        // TODO restart logstash!
    }

    public String getCurrentContainerId() {
        return currentContainerId;
    }

    // TODO move out into executor?? We only need a reference to the Template. Pass that to constructor instead
    private void initTemplatingEngine() {
        Configuration conf = new Configuration();
        try {
            conf.setDirectoryForTemplateLoading(new File("/mnt/mesos/sandbox"));
        }
        catch(IOException e) {
            LOGGER.error("Failed to set template directory");
        }
        conf.setDefaultEncoding("UTF-8");
        conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        try {
            configTemplate = conf.getTemplate("config.template");
        }
        catch(IOException e) {
            LOGGER.error("Couldn't load config tempate!");
        }
    }
}
