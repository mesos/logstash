package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.DockerClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Class responsible for connecting each discovered framework to logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashConnector implements FrameworkListener {

    private String logstashContainer;

    private Map<String, LogForwarder> forwarders;

    private LogstashService logstash;

    private DockerInfo client;

    public LogstashConnector(DockerInfo client) {
        this.client = client;
        logstash = new LogstashService(client);
        forwarders = new HashMap<String, LogForwarder>();
    }

    public void frameworkAdded(Framework f) {
        logstash.stop();
        try {
            forwarders.put(f.getId(), new LogForwarder(f));
        }
        finally {
            logstash.start();
        }

        runForwarders();
    }

    public void frameworkRemoved(Framework f) {
        logstash.stop();
        try {
            forwarders.remove(f.getId());
        }
        finally {
            logstash.start();
        }

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
            // TODO
        }
    }
}
