package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.container.AbstractContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
        return concatLists(
                asList(
    //                "-Xmx256m",
                    "-Dmesos.logstash.web.port=" + apiPort,
                    "-Dmesos.logstash.framework.name=logstash",
                    "-Dmesos.logstash.logstash.heap.size=128",
                    "-Dmesos.logstash.executor.heap.size=64",
                    "-Dmesos.logstash.volumes=/var/log/mesos",
                    "-Dmesos.zk=zk://" + zookeeperIpAddress + ":2181/mesos"
            ),
            optionalToList(elasticsearchDomainAndPort.map(d -> "-Dmesos.logstash.elasticsearchDomainAndPort=" + d))
        );
    }

    private static <T> List<T> concatLists(List<T> a, List<T> b) {
        ArrayList<T> r = new ArrayList<T>();
        r.addAll(a);
        r.addAll(b);
        return r;
    }

    private static <T> List<T> optionalToList(Optional<T> ox) {
        return ox.map(x -> asList(x)).orElse(asList());
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
                .withExposedPorts(ExposedPort.tcp(apiPort));
    }
}
