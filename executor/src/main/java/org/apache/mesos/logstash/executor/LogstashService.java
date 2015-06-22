package org.apache.mesos.logstash.executor;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash
 * Created by peldan on 22/06/15.
 */
public class LogstashService {

    public static final String LOGSTASH_IMAGE = "logstash";

    private DockerInfo client;
    private String currentContainerId;

    public LogstashService(DockerInfo client) {
        this.client = client;
    }

    public void start() {
        if(currentContainerId == null) {
            String containerId = client.startContainer(LOGSTASH_IMAGE);
        }
    }

    public void stop() {
        if(currentContainerId != null) {
            client.stopContainer(currentContainerId);
            currentContainerId = null;
        }
    }

    public String getCurrentContainerId() {
        return currentContainerId;
    }
}
