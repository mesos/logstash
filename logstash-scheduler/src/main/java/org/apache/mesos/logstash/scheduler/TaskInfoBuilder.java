package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Value.Type;
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
import java.util.ArrayList;
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
    private FrameworkConfig frameworkConfig;
    @Inject
    private ExecutorConfig executorConfig;
    @Inject
    private LogstashConfig logstashConfig;

    @SuppressWarnings("unchecked")
    public Protos.TaskInfo buildTask(Protos.Offer offer) {
        Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo
            .newBuilder()
            .setForcePullImage(false)
            .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
            .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG);

        List<Integer> blackList = new ArrayList<>();
        if (features.isSyslog()) {
            if (frameworkConfig.getSyslogPort() != null) {
                dockerExecutor.addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(frameworkConfig.getSyslogPort()).setContainerPort(frameworkConfig.getSyslogPort()).setProtocol("udp"));
            } else {
                int syslogPort = selectPortFromRange(offer.getResourcesList(), blackList);
                dockerExecutor.addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(syslogPort).setContainerPort(syslogPort).setProtocol("udp"));
                blackList.add(syslogPort);
            }
        }
        if (features.isCollectd()) {
            if (frameworkConfig.getCollectdPort() != null) {
                dockerExecutor.addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(frameworkConfig.getCollectdPort()).setContainerPort(frameworkConfig.getCollectdPort()).setProtocol("udp"));
            }
            int syslogPort = selectPortFromRange(offer.getResourcesList(), blackList);
            dockerExecutor.addPortMappings(Protos.ContainerInfo.DockerInfo.PortMapping.newBuilder().setHostPort(syslogPort).setContainerPort(syslogPort).setProtocol("udp"));
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
                    .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG).build())
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
        logstashConfig.getElasticsearchUrl().ifPresent(hostAndPort -> logstashConfigBuilder.setLogstashPluginOutputElasticsearch(LogstashProtos.LogstashPluginOutputElasticsearch.newBuilder().setHost(hostAndPort)));

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

    public int selectPortFromRange(List<Protos.Resource> offeredResources, List<Integer> blackList) {
        for (Protos.Resource offeredResource : offeredResources) {
            if (offeredResource.getType().equals(Type.RANGES)) {
                int port = (int) offeredResource.getRanges().getRange(0).getBegin();
                if (blackList.contains(port)) {
                    continue;
                }
                return port;
            }
        }
        return 0;
    }

    public List<Protos.Resource> getResourcesList() {
        int memNeeded = executorConfig.getHeapSize() + logstashConfig.getHeapSize() + executorConfig.getOverheadMem();

        return asList(
            Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(executorConfig.getCpus()).build())
                .build(),
            Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memNeeded).build())
                .build()
        );
    }

    private String formatTaskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(LogstashConstants.TASK_DATE_FORMAT).format(clock.now());
        return LogstashConstants.FRAMEWORK_NAME + "_" + offer.getHostname() + "_" + date;
    }

}
