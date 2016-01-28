package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import org.elasticsearch.common.lang3.StringUtils;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * Container for the Logstash scheduler
 */
public class LogstashSchedulerContainer extends AbstractContainer {

    public static final String SCHEDULER_IMAGE = "mesos/logstash-scheduler";

    public static final String SCHEDULER_NAME = "logstash-scheduler";

    private String zookeeperIpAddress;
    private Optional<String> elasticsearchUrl = Optional.empty();
    private boolean withSyslog = false;
    private final Optional<String> mesosRole;
    private boolean useDocker = true;

    @Override
    public void start(int timeout) {
        super.start(timeout);
        System.out.println("LogstashSchedulerContainer.start");
    }

    public LogstashSchedulerContainer(DockerClient dockerClient, String zookeeperIpAddress, String mesosRole, String elasticsearchUrl) {
        super(dockerClient);
        this.zookeeperIpAddress = zookeeperIpAddress;
        this.mesosRole = Optional.ofNullable(mesosRole);
        this.elasticsearchUrl = Optional.ofNullable(elasticsearchUrl);
    }

    private List<String> getJavaOpts() {
        return mergeWithOptionals(
                asList(
                        "-Dmesos.logstash.framework.name=logstash",
                        "-Dmesos.logstash.volumes=/var/log/mesos"
                ),
                elasticsearchUrl.map(d -> "-Dmesos.logstash.elasticsearchDomainAndPort=" + d)
        );
    }

    @SafeVarargs
    private static <T> List<T> mergeWithOptionals(List<T> list, Optional<T> ... optionals) {
        return Stream.concat(
                list.stream(),
                Arrays.stream(optionals).filter(Optional::isPresent).map(Optional::get)
        ).collect(Collectors.toList());
    }

    @Override
    protected void pullImage() {
        dockerClient.pullImageCmd(SCHEDULER_IMAGE);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        final String cmd = asList(
                "--zk-url=zk://" + zookeeperIpAddress + ":2181/mesos",
                mesosRole.map(role -> "--mesos-role=" + role).orElse(null),
                "--failover-enabled=false",
                elasticsearchUrl.map(url -> "--logstash.elasticsearch-url=" + url).orElse(null),
                "--executor.heap-size=64",
                "--logstash.heap-size=256",
                "--enable.docker=" + useDocker,
                withSyslog ? "--enable.syslog=true" : null
        ).stream().filter(StringUtils::isNotEmpty).collect(Collectors.joining(" "));
        System.out.println("cmd = " + cmd);
        return dockerClient
                .createContainerCmd(SCHEDULER_IMAGE)
                .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=" + getJavaOpts().stream().collect(Collectors.joining(" ")))
                .withExposedPorts(ExposedPort.tcp(9092))
                .withCmd(cmd);
    }

    public void enableSyslog() {
        withSyslog = true;
    }

    public void setDocker(boolean useDocker) {
      this.useDocker = useDocker;
    }
}
