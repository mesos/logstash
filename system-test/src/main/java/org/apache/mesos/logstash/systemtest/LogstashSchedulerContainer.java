package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
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

    public LogstashSchedulerContainer(DockerClient dockerClient, String ipAddress) {
        super(dockerClient);
        this.ipAddress = ipAddress;
        this.javaOpts = asList(
                "-Xmx256m",
                "-Dmesos.logstash.web.port=9092",
                "-Dmesos.logstash.framework.name=logstash",
                "-Dmesos.logstash.logstash.heap.size=512",
                "-Dmesos.logstash.executor.heap.size=256",
                "-Dmesos.logstash.volumes=/var/log/mesos",
                "-Dmesos.zk=zk://" + ipAddress + ":2181/mesos"
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
                .withBinds(Bind.parse(System.getProperty("user.dir") + "/logstash-scheduler/build/libs/:/tmp/"))
                .withEnv("JAVA_OPTS=" + StringUtils.collectionToDelimitedString(javaOpts, " "));
    }
}
