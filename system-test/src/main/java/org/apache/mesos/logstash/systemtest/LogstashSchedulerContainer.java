package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import org.springframework.util.StringUtils;

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
    private final int apiPort = 9092;
    private Optional<String> elasticsearchDomainAndPort = Optional.empty();
    private boolean withSyslog = false;

    public LogstashSchedulerContainer(DockerClient dockerClient, String zookeeperIpAddress) {
        super(dockerClient);
        this.zookeeperIpAddress = zookeeperIpAddress;
    }

    public LogstashSchedulerContainer(DockerClient dockerClient, String zookeeperIpAddress, String elasticsearchDomainAndPort) {
        super(dockerClient);
        this.zookeeperIpAddress = zookeeperIpAddress;
        this.elasticsearchDomainAndPort = Optional.ofNullable(elasticsearchDomainAndPort);
    }

    private List<String> getJavaOpts() {
        return mergeWithOptionals(
                asList(
                        "-Dmesos.logstash.web.port=" + apiPort,
                        "-Dmesos.logstash.framework.name=logstash",
                        "-Dmesos.logstash.disableFailover=true",
                        "-Dmesos.logstash.logstash.heap.size=128",
                        "-Dmesos.logstash.executor.heap.size=64",
                        "-Dmesos.logstash.volumes=/var/log/mesos",
                        "-Dmesos.zk=zk://" + zookeeperIpAddress + ":2181/mesos"
                ),
                elasticsearchDomainAndPort.map(d -> "-Dmesos.logstash.elasticsearchDomainAndPort=" + d)
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
        return dockerClient
                .createContainerCmd(SCHEDULER_IMAGE)
                .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                .withEnv("JAVA_OPTS=" + StringUtils.collectionToDelimitedString(getJavaOpts(), " "))
                .withExposedPorts(ExposedPort.tcp(apiPort))
                .withCmd(withSyslog ? "--enable.syslog=true" : "");
    }

    public void enableSyslog() {
        withSyslog = true;
    }
}
