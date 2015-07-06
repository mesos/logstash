package org.apache.mesos.logstash.scheduler;

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

    // one-to-one map of slave and corresponding executor
    private Map<Protos.SlaveID, Protos.ExecutorID> executors;
    private Map<String, String> dockerConfigurations;
    private Map<String, String> hostConfigurations;

    // As per the DCOS Service Specification, setting the failover timeout to a large value;
    private static final double FAILOVER_TIMEOUT = 86400000;
    private Protos.FrameworkID frameworkId;

    public Scheduler(String masterURL, String executorImageName) {
        this.masterURL = masterURL;
        this.executorImageName = executorImageName;
        this.driver = buildSchedulerDriver();
        this.executors = new HashMap<>();

        ConfigMonitor dockerMonitor = new ConfigMonitor("config/docker");
        dockerMonitor.start(this::newDockerConfigAvailable);

        ConfigMonitor hostMonitor = new ConfigMonitor("config/host");
        hostMonitor.start(this::newHostConfigAvailable);
    }

    private MesosSchedulerDriver buildSchedulerDriver() {
        final Protos.FrameworkInfo frameworkInfo = buildFramework();

        LOGGER.info("Connecting to masterURL " + getMesosUrl());

        return new MesosSchedulerDriver(this, frameworkInfo, getMesosUrl());
    }

    private void newDockerConfigAvailable(Map<String, String> config) {
        LOGGER.info("New docker config!");
        this.dockerConfigurations = config;
        broadcastConfig(dockerConfigurations, hostConfigurations);
    }

    private void newHostConfigAvailable(Map<String, String> config) {
        LOGGER.info("New host config!");
        this.hostConfigurations = config;
        broadcastConfig(dockerConfigurations, hostConfigurations);
    }

    private void broadcastConfig(Map<String, String> dockerConfigurations, Map<String, String> hostConfigurations) {
        if(dockerConfigurations == null || hostConfigurations == null) {
            LOGGER.info("Skipping broadcast, haven't read all configs yet");
            return;
        }
        LOGGER.info("Broadcasting configuration change");
        byte[] message = configMapToByteArray(dockerConfigurations, hostConfigurations);
        for(Map.Entry<Protos.SlaveID, Protos.ExecutorID> entry : executors.entrySet()) {
            LOGGER.info("Message sent to " + entry.getKey());
            driver.sendFrameworkMessage(entry.getValue(), entry.getKey(), message);
        }
    }

    private static byte[] configMapToByteArray(Map<String, String> dockerConfigurations, Map<String, String> hostConfigurations) {
        LogstashProtos.SchedulerMessage.Builder builder = LogstashProtos.SchedulerMessage.newBuilder();

        for(Map.Entry<String, String> entry : dockerConfigurations.entrySet()) {
            builder.addDockerConfig(LogstashProtos.LogstashConfig.newBuilder()
                    .setConfig(entry.getValue())
                    .setFrameworkName(entry.getKey()));
        }

        for(Map.Entry<String, String> entry : hostConfigurations.entrySet()) {
            builder.addHostConfig(LogstashProtos.LogstashConfig.newBuilder()
                    .setConfig(entry.getValue())
                    .setFrameworkName(entry.getKey()));
        }

        return builder.build().toByteArray();

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

                LOGGER.info("Launching task..");
                tasks.add(new Task(offer.getHostname(), id));
                schedulerDriver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                LOGGER.info("Task launched.");
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

        if(taskStatus.getState() == Protos.TaskState.TASK_RUNNING) {
            LOGGER.info("Slave " + taskStatus.getSlaveId() + ", executor " + taskStatus.getExecutorId());
            executors.put(taskStatus.getSlaveId(), taskStatus.getExecutorId());

            // Tell the new instance of our configuration
            byte[] msg = configMapToByteArray(dockerConfigurations, hostConfigurations);
            schedulerDriver.sendFrameworkMessage(taskStatus.getExecutorId(), taskStatus.getSlaveId(), msg);
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
        LOGGER.info("Message received");
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
        LOGGER.error("It broke. " + s);
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
                .setForcePullImage(false)
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
