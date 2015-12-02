package org.apache.mesos.logstash.scheduler;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.ExecutorEnvironmentalVariables;
import org.apache.mesos.logstash.util.Clock;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class TaskInfoBuilder {

    private final Configuration configuration;
    private final Clock clock;

    public TaskInfoBuilder(Configuration configuration) {
        this.configuration = configuration;
        this.clock = new Clock();
    }

    public Protos.TaskInfo buildTask(Protos.Offer offer) {

        Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo
            .newBuilder()
            .setForcePullImage(false)
            .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
            .addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(514).setContainerPort(514).setProtocol("udp"))
            .addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(5000).setContainerPort(5000).setProtocol("udp"))
            .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG);

        Protos.ContainerInfo.Builder container = Protos.ContainerInfo.newBuilder()
            .setType(Protos.ContainerInfo.Type.DOCKER)
            .addAllVolumes(getVolumes())
            .setDocker(dockerExecutor.build());

        ExecutorEnvironmentalVariables executorEnvVars = new ExecutorEnvironmentalVariables(
            configuration);

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
            .setName(LogstashConstants.NODE_NAME + " executor")
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue("executor." + UUID.randomUUID()))
            .setContainer(container)
            .setCommand(Protos.CommandInfo.newBuilder()
                .addArguments("dummyArgument")
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder()
                    .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG).build())
                .setEnvironment(Protos.Environment.newBuilder()
                    .addAllVariables(executorEnvVars.getList()))
                .setShell(false))
            .build();


        final LogstashProtos.LogstashConfiguration.Builder logstashConfigBuilder = LogstashProtos.LogstashConfiguration.newBuilder();
        logstashConfigBuilder.setLogstashPluginInputSyslog(
                LogstashProtos.LogstashPluginInputSyslog.newBuilder()
                        .setPort(514) // FIXME take from config
        );
        configuration.getElasticsearchDomainAndPort().ifPresent(hostAndPort -> logstashConfigBuilder.setLogstashPluginOutputElasticsearch(LogstashProtos.LogstashPluginOutputElasticsearch.newBuilder().setHost(hostAndPort)));
        LogstashProtos.LogstashConfiguration logstashConfiguration = logstashConfigBuilder.build();

        return Protos.TaskInfo.newBuilder()
            .setExecutor(executorInfo)
            .addAllResources(getResourcesList())
            .setName(LogstashConstants.TASK_NAME)
            .setTaskId(Protos.TaskID.newBuilder().setValue(formatTaskId(offer)))
            .setSlaveId(offer.getSlaveId())
            .setData(logstashConfiguration.toByteString())
            .build();
    }

    private List<Protos.Volume> getVolumes() {
        return configuration.getVolumes().stream().map(s ->
                        Protos.Volume.newBuilder()
                                .setHostPath(s)
                                .setContainerPath(generateVolumeContainerPath(s))
                                .setMode(Protos.Volume.Mode.RO)
                                .build()
        ).collect(Collectors.toList());
    }

    private static String generateVolumeContainerPath(String hostPath) {
        return Paths.get(LogstashConstants.VOLUME_MOUNT_DIR, hostPath).toString();
    }


    public List<Protos.Resource> getResourcesList() {

        int memNeeded = configuration.getExecutorHeapSize() + configuration.getLogstashHeapSize() + configuration.getExecutorOverheadMem();

        return Arrays.asList(
            Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(configuration.getExecutorCpus()).build())
                .build(),
            Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(memNeeded).build())
                .build(),
                Protos.Resource.newBuilder()
                .setName("ports")
                .setType(Protos.Value.Type.RANGES)
                        .setRanges(Protos.Value.Ranges.newBuilder()
                                .addRange(Protos.Value.Range.newBuilder().setBegin(514).setEnd(514))
                                .addRange(Protos.Value.Range.newBuilder().setBegin(5000).setEnd(5000))
                        )
                        .build()
        );
    }
    private String formatTaskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(LogstashConstants.TASK_DATE_FORMAT).format(clock.now());
        return LogstashConstants.FRAMEWORK_NAME + "_" + offer.getHostname() + "_" + date;
    }



}
