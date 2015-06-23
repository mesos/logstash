package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.DockerClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Class responsible for connecting each discovered framework to logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashConnector implements FrameworkListener {

    public static final Logger LOGGER = Logger.getLogger(LogstashConnector.class.toString());

    private String logstashContainer;

    private Map<String, LogForwarder> forwarders;

    private LogstashService logstash;

    private DockerInfo client;

    private DockerPoll poll;

    public LogstashConnector(DockerInfo client) {
        this.client = client;
        logstash = new LogstashService(client);
        forwarders = new HashMap<>();
    }

    public void init() {
        LOGGER.info("Hello, world!");

        poll = new DockerPoll(client);
        poll.attach(this);
    }

    public void frameworkAdded(Framework f) {
        LOGGER.info("New framework. stopping logstash..");

        logstash.stop();
        try {
            forwarders.put(f.getId(), new LogForwarder(f));
        }
        finally {
            LOGGER.info("Starting again!");
            logstash.start();
        }

        LOGGER.info("..and forwarders!");
        runForwarders();
    }

    public void frameworkRemoved(Framework f) {
        LOGGER.info("Framework removed");

        logstash.stop();
        try {
            forwarders.remove(f.getId());
        }
        finally {
            LOGGER.info("Starting again!");
            logstash.start();
        }

        LOGGER.info("..and forwarders");
        runForwarders();
    }


    private void runForwarders() {
        for(LogForwarder f : forwarders.values()) {
            f.start();
        }
    }


    class LogForwarder {
        Framework framework;

        LogForwarder(Framework framework) {
            this.framework = framework;
        }

        void start() {
            LOGGER.info("Running magic command");
            client.execInContainer(framework.getId(), "echo 'I was here but I dissapear' >> /dev/stdout");
        }
    }
}
