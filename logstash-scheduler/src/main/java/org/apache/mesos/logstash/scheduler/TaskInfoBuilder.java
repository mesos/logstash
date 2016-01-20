package org.apache.mesos.logstash.scheduler;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.config.ExecutorConfig;
import org.apache.mesos.logstash.config.ExecutorEnvironmentalVariables;
import org.apache.mesos.logstash.config.FrameworkConfig;
import org.apache.mesos.logstash.config.LogstashConfig;
import org.apache.mesos.logstash.util.Clock;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.UUID;

import static java.util.Arrays.asList;

@Component
public class TaskInfoBuilder {

    @Inject
    private Clock clock;
    @Inject
    private Features features;
    @Inject
    private ExecutorConfig executorConfig;
    @Inject
    private LogstashConfig logstashConfig;
    @Inject
    private FrameworkConfig frameworkConfig;

    public Protos.TaskInfo buildTask(Protos.Offer offer) {

        String executorImage = logstashConfig.getExecutorImage() + ":" + logstashConfig.getExecutorVersion();

        Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo
            .newBuilder()
            .setForcePullImage(false)
            .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
            .setImage(executorImage);

        if (features.isSyslog()) {
            dockerExecutor.addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(514).setContainerPort(514).setProtocol("udp"));
        }
        if (features.isCollectd()) {
            dockerExecutor.addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(25826).setContainerPort(25826).setProtocol("udp"));
        }

        Protos.ContainerInfo.Builder container = Protos.ContainerInfo.newBuilder()
            .setType(Protos.ContainerInfo.Type.DOCKER)
            .setDocker(dockerExecutor.build());
        if (features.isFile()) {
            container.addVolumes(Protos.Volume.newBuilder().setHostPath("/").setContainerPath("/logstashpaths").setMode(Protos.Volume.Mode.RO).build());
        }

        ExecutorEnvironmentalVariables executorEnvVars = new ExecutorEnvironmentalVariables(
                executorConfig, logstashConfig);

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
            .setName(LogstashConstants.NODE_NAME + " executor")
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue("executor." + UUID.randomUUID()))
            .setContainer(container)
            .setCommand(Protos.CommandInfo.newBuilder()
                .addArguments("dummyArgument")
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder()
                    .setImage(executorImage).build())
                .setEnvironment(Protos.Environment.newBuilder()
                    .addAllVariables(executorEnvVars.getList()))
                .setShell(false))
            .build();


        final LogstashProtos.LogstashConfiguration.Builder logstashConfigBuilder = LogstashProtos.LogstashConfiguration.newBuilder();
        if (features.isSyslog()) {
            logstashConfigBuilder.setLogstashPluginInputSyslog(
                    LogstashProtos.LogstashPluginInputSyslog.newBuilder().setPort(514) // FIXME take from config
            );
        }
        //TODO: repeat for collectd
        logstashConfig.getElasticsearchUrl().ifPresent(url -> logstashConfigBuilder.setLogstashPluginOutputElasticsearch(LogstashProtos.LogstashPluginOutputElasticsearch.newBuilder().setUrl(url.toExternalForm())));

        logstashConfigBuilder.setMesosSlaveId(offer.getSlaveId().getValue());

        if (features.isFile()) {
            logstashConfigBuilder.setLogstashPluginInputFile(
                    LogstashProtos.LogstashPluginInputFile.newBuilder().addAllPath(executorConfig.getFilePath())
            );
        }


        return Protos.TaskInfo.newBuilder()
            .setExecutor(executorInfo)
            .addAllResources(getResourcesList())
            .setName(LogstashConstants.TASK_NAME)
            .setTaskId(Protos.TaskID.newBuilder().setValue(formatTaskId(offer)))
            .setSlaveId(offer.getSlaveId())
            .setData(logstashConfigBuilder.build().toByteString())
            .build();
    }

    public List<Protos.Resource> getResourcesList() {

        int memNeeded = executorConfig.getHeapSize() + logstashConfig.getHeapSize() + executorConfig.getOverheadMem();

        return asList(
            Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(executorConfig.getCpus()).build())
                .build(),
            Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memNeeded).build())
                .build(),
            Protos.Resource.newBuilder()
                .setName("ports")
                .setRole(frameworkConfig.getMesosRole())
                .setType(Protos.Value.Type.RANGES)
                .setRanges(mapSelectedPortRanges())
                .build()
        );
    }

    private Protos.Value.Ranges.Builder mapSelectedPortRanges() {
        Protos.Value.Ranges.Builder rangesBuilder = Protos.Value.Ranges.newBuilder();
        if (features.isSyslog()) {
            rangesBuilder.addRange(Protos.Value.Range.newBuilder().setBegin(514).setEnd(514));
        }
        if (features.isCollectd()) {
            rangesBuilder.addRange(Protos.Value.Range.newBuilder().setBegin(25826).setEnd(25826));
        }
        return rangesBuilder;
    }

    private String formatTaskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(LogstashConstants.TASK_DATE_FORMAT).format(clock.now());
        return LogstashConstants.FRAMEWORK_NAME + "_" + offer.getHostname() + "_" + date;
    }



}
