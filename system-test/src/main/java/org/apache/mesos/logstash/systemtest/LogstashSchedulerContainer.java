package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import org.elasticsearch.common.lang3.StringUtils;

import java.security.SecureRandom;
import java.util.Optional;

import static java.util.Arrays.asList;

/**
 * Container for the Logstash scheduler
 */
public class LogstashSchedulerContainer extends AbstractContainer {

    public static final String SCHEDULER_IMAGE = "mesos/logstash-scheduler";

    public static final String SCHEDULER_NAME = "logstash-scheduler";

    private String zookeeperIpAddress;
    private Optional<String> elasticsearchHost = Optional.empty();
    private boolean withSyslog = false;
    private final Optional<String> mesosRole;
    private boolean useDocker = true;

    public LogstashSchedulerContainer(DockerClient dockerClient, String zookeeperIpAddress, String mesosRole, String elasticsearchHost) {
        super(dockerClient);
        this.zookeeperIpAddress = zookeeperIpAddress;
        this.mesosRole = Optional.ofNullable(mesosRole);
        this.elasticsearchHost = Optional.ofNullable(elasticsearchHost);
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(SCHEDULER_IMAGE);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        final String[] cmd = asList(
                "--zk-url=zk://" + zookeeperIpAddress + ":2181/mesos",
                mesosRole.map(role -> "--mesos-role=" + role).orElse(null),
                "--enable.failover=false",
                elasticsearchHost.map(host -> "--logstash.elasticsearch-host=" + host).orElse(null),
                "--executor.heap-size=64",
                "--logstash.heap-size=256",
                "--enable.docker=" + useDocker,
                withSyslog ? "--enable.syslog=true" : null
        ).stream().filter(StringUtils::isNotEmpty).toArray(String[]::new);
        return dockerClient
                .createContainerCmd(SCHEDULER_IMAGE)
                .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                .withExposedPorts(ExposedPort.tcp(9092))
                .withCmd(cmd);
    }

    public void enableSyslog() {
        withSyslog = true;
    }

    public void setDocker(boolean useDocker) {
      this.useDocker = useDocker;
    }

    @Override
    public String getRole() {
        return SCHEDULER_NAME;
    }

}
