package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.LogstashProtos;

import java.text.SimpleDateFormat;
import java.util.*;

public class Scheduler implements org.apache.mesos.Scheduler, Runnable {

    public static final Logger LOGGER = Logger.getLogger(Scheduler.class);

    private static final int MESOS_PORT = 5050;

    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    private Clock clock = new Clock();
    private Set<Task> tasks = new HashSet<>();

    private final String masterURL; // the URL of the mesos masterURL
    private final String executorImageName;

    private final MesosSchedulerDriver driver;

    // As per the DCOS Service Specification, setting the failover timeout to a large value;
    private static final double FAILOVER_TIMEOUT = 86400000;
    private Protos.FrameworkID frameworkId;

    public Scheduler(String masterURL, String executorImageName) {
        this.masterURL = masterURL;
        this.executorImageName = executorImageName;
        this.driver = buildSchedulerDriver();
    }

    private MesosSchedulerDriver buildSchedulerDriver() {
        final Protos.FrameworkInfo frameworkInfo = buildFramework();

        LOGGER.info("Connecting to masterURL " + getMesosUrl());

        return new MesosSchedulerDriver(this, frameworkInfo, getMesosUrl());
    }

    private Protos.FrameworkInfo buildFramework() {
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setName(Configuration.FRAMEWORK_NAME);
        frameworkBuilder.setUser("root"); // TODO: change, (thb) meaning what
        frameworkBuilder.setCheckpoint(true);
        frameworkBuilder.setFailoverTimeout(FAILOVER_TIMEOUT);
        return frameworkBuilder.build();
    }

    public String getMesosUrl() {
        return this.masterURL + ":" + MESOS_PORT;
    }

    @Override
    public void run() {
        driver.run();
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        LOGGER.info("Registered against Mesos");
        this.frameworkId = frameworkID;
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Reregistered against Mesos");
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {
        for (Protos.Offer offer : list) {
            LOGGER.info("Received offer from slave " + offer.getSlaveId() + ", " + offer.getHostname());

            if (shouldAcceptOffer(offer)) {
                LOGGER.info("Offer accepted");

                String id = taskId(offer);
                Protos.TaskInfo taskInfo = buildTask(schedulerDriver, offer, id);

                schedulerDriver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                tasks.add(new Task(offer.getHostname(), id));
            } else {
                LOGGER.info("Offer declined");
                schedulerDriver.declineOffer(offer.getId());
            }
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        LOGGER.info("Offer rescinded");

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        LOGGER.info("Task status update! " + taskStatus.toString());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
        LOGGER.info("Message received");

        try {
            LogstashProtos.ExecutorMessage executorMessage = LogstashProtos.ExecutorMessage.parseFrom(bytes);
            List<String> frameworkNames = executorMessage.getFrameworkNameList();

            for (String frameworkName : frameworkNames) {
                LOGGER.info(String.format("Framework name: %s", frameworkName));
            }

            byte[] message = createExecutorMessage(frameworkNames);

            schedulerDriver.sendFrameworkMessage(executorID, slaveID, message);

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private byte[] createExecutorMessage(List<String> frameworkNames) {

        List<LogstashProtos.LogstashConfig> logstashConfigs = new ArrayList<>();

        for (String frameworkName : frameworkNames) {
            logstashConfigs.add(LogstashProtos.LogstashConfig.newBuilder()
                    .addLogInputConfiguraton(LogstashProtos.LogInputConfiguration.newBuilder()
                            .setLocation("/var/log/apt/history.log")
                            .setTag("SOME TAG").setType("SOME TYPE")
                            .build())
                    .setFrameworkName(frameworkName).build());
        }

        return LogstashProtos.SchedulerMessage.newBuilder()
                .setConfigurationFragments("{{}}")
                .addAllLogstashConfig(logstashConfigs)
                .build().toByteArray();
    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        LOGGER.error("Disconnected :(");
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
        LOGGER.error("Slave losta");
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
        LOGGER.error("Executor losta");
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {
        LOGGER.error("It broke.");
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format(Configuration.FRAMEWORK_NAME + "_%s_%s", offer.getHostname(), date);
    }

    private Protos.TaskInfo buildTask(SchedulerDriver driver, Protos.Offer offer, String id) {

        LOGGER.info("BUILDING task!");
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(Configuration.TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(offer.getResourcesList());

        Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo.newBuilder()
                .setForcePullImage(true)
                .setImage(executorImageName.replace("docker.io/", ""));


        LOGGER.info("Using Executor to start Logstash cloud mesos on slaves");

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(frameworkId)
                .setContainer(Protos.ContainerInfo.newBuilder().setType(Protos.ContainerInfo.Type.DOCKER).setDocker(dockerExecutor.build()))
                .setName("" + UUID.randomUUID())
                .setCommand(Protos.CommandInfo.newBuilder()
                        .addArguments("java")
                        .addArguments("-Djava.library.path=/usr/local/lib")
                        .addArguments("-jar")
                        .addArguments("/tmp/logstash-executor.jar")
                        .setShell(false))
                .build();

        taskInfoBuilder.setExecutor(executorInfo);

        return taskInfoBuilder.build();
    }

    private boolean shouldAcceptOffer(Protos.Offer offer) {
        // Don't start the same framework multiple times on the same host
        for (Task task : tasks) {
            if (task.getHostname().equals(offer.getHostname())) {
                return false;
            }
        }
        return true;
    }

    public void stop() {
        // FIXME: Is the driver thread safe?
        driver.stop();
    }
}
