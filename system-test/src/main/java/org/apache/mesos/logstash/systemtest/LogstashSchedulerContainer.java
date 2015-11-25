package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import org.springframework.util.StringUtils;

import java.security.SecureRandom;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;

/**
 * Container for the Logstash scheduler
 */
public class LogstashSchedulerContainer extends AbstractContainer {

    public static final String SCHEDULER_IMAGE = "mesos/logstash-scheduler";

    public static final String SCHEDULER_NAME = "logstash-scheduler";

    private List<String> javaOpts;

    private String ipAddress;
    private final int apiPort = 9092;

    public LogstashSchedulerContainer(DockerClient dockerClient, String zookeeperIpAddress) {
        super(dockerClient);
        this.ipAddress = zookeeperIpAddress;
        this.javaOpts = asList(
//                "-Xmx256m",
                "-Dmesos.logstash.web.port=" + apiPort,
                "-Dmesos.logstash.framework.name=logstash",
                "-Dmesos.logstash.logstash.heap.size=128",
                "-Dmesos.logstash.executor.heap.size=64",
                "-Dmesos.logstash.volumes=/var/log/mesos",
                "-Dmesos.zk=zk://" + zookeeperIpAddress + ":2181/mesos"
        );
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
                .withEnv("JAVA_OPTS=" + StringUtils.collectionToDelimitedString(javaOpts, " "))
                .withExposedPorts(ExposedPort.tcp(apiPort));
    }
}
