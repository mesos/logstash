package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.mini.container.AbstractContainer;

import java.security.SecureRandom;
import java.util.stream.IntStream;

/**
 * Container for the Logstash scheduler
 */
public class LogstashSchedulerContainer extends AbstractContainer {

    public static final String SCHEDULER_IMAGE = "mesos/logstash-scheduler";

    public static final String SCHEDULER_NAME = "logstash-scheduler";

    private String ipAddress;

    public LogstashSchedulerContainer(DockerClient dockerClient, String ipAddress) {
        super(dockerClient);
        this.ipAddress = ipAddress;
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(SCHEDULER_IMAGE);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient
                .createContainerCmd(SCHEDULER_IMAGE)
                .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=-Xmx256m -Dmesos.zk=zk://" + ipAddress + ":2181/mesos -Dmesos.logstash.framework.name=logstash")
                .withExtraHosts(IntStream.rangeClosed(1, 3).mapToObj(value -> "slave" + value + ":" + ipAddress).toArray(String[]::new));
    }
}
