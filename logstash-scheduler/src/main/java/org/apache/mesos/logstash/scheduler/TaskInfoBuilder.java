package org.apache.mesos.logstash.scheduler;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.ExecutorEnvironmentalVariables;
import org.apache.mesos.logstash.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class TaskInfoBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashScheduler.class);

    private final Configuration configuration;
    private final Clock clock;

    public TaskInfoBuilder(Configuration configuration) {
        this.configuration = configuration;
        this.clock = new Clock();
    }

    public Protos.TaskInfo buildTask(Protos.Offer offer) {
        Protos.ExecutorInfo executorInfo;

        if (configuration.isFrameworkUseDocker()) {
            Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo
                    .newBuilder()
                    .setForcePullImage(false)
                    .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG);

            Protos.ContainerInfo.Builder container = Protos.ContainerInfo.newBuilder()
                    .setType(Protos.ContainerInfo.Type.DOCKER)
                    .addAllVolumes(getVolumes())
                    .setDocker(dockerExecutor.build());

            ExecutorEnvironmentalVariables executorEnvVars = new ExecutorEnvironmentalVariables(
                    configuration);

            executorInfo = Protos.ExecutorInfo.newBuilder()
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
        } else {
            ExecutorEnvironmentalVariables executorEnvVars = new ExecutorEnvironmentalVariables(
                    configuration);

            Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                    .setEnvironment(Protos.Environment.newBuilder().addAllVariables(executorEnvVars.getList()));

            String address = configuration.getFrameworkFileServerAddress();
            if (address == null) {
                throw new NullPointerException("Webserver address is null");
            }
            String httpPath =  address + "/get/" + Configuration.LOGSTASH_EXECUTOR_JAR;
            LOGGER.debug("Using file server: " + httpPath);
            commandInfoBuilder
                    .setValue(configuration.getJavaHome() + "java $JAVA_OPTS -jar ./" + Configuration.LOGSTASH_EXECUTOR_JAR)
                    .addUris(Protos.CommandInfo.URI.newBuilder().setValue(httpPath));

            executorInfo = Protos.ExecutorInfo.newBuilder()
                    .setName(LogstashConstants.NODE_NAME + " executor")
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue("executor." + UUID.randomUUID()))
                    .setCommand(commandInfoBuilder)
                    .build();
        }

        return Protos.TaskInfo.newBuilder()
                .setExecutor(executorInfo)
                .addAllResources(getResourcesList())
                .setName(LogstashConstants.TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(formatTaskId(offer)))
                // TODO (thb) Consider using setData to pass the current config.
                // This would prevent a round trip, asking the scheduler for it.
                // e.g. .setData(latestConfig.toByteString())
                .setSlaveId(offer.getSlaveId())
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
                .build()
        );
    }
    private String formatTaskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(LogstashConstants.TASK_DATE_FORMAT).format(clock.now());
        return LogstashConstants.FRAMEWORK_NAME + "_" + offer.getHostname() + "_" + date;
    }



}
