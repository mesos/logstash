package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ContainerInfo.Type;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.cluster.ClusterMonitor;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.ExecutorEnvironmentalVariables;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedCollection;
import static org.apache.mesos.Protos.*;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage.SchedulerMessageType.NEW_CONFIG;

@Component
public class LogstashScheduler implements org.apache.mesos.Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashScheduler.class);

    private final ConfigManager configManager;
    private final Collection<FrameworkMessageListener> listeners;
    private final LiveState liveState;
    private final Clock clock;
    private final Configuration configuration;

    private MesosSchedulerDriver driver;
    private boolean registered;
    private ClusterMonitor clusterMonitor = null;
    private Observable statusUpdateWatchers = new StatusUpdateObservable();

    @Autowired
    public LogstashScheduler(
        LiveState liveState,
        Configuration configuration,
        ConfigManager configManager) {
        this.liveState = liveState;

        this.configuration = configuration;
        this.configManager = configManager;

        this.listeners = synchronizedCollection(new ArrayList<>());
        this.clock = new Clock();
    }

    @PostConstruct
    public void start() {
        configManager.setOnConfigUpdate(this::updateExecutorConfig);

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(configuration.getFrameworkName())
            .setUser(configuration.getLogStashUser())
            .setRole(configuration.getLogStashRole())
            .setCheckpoint(true)
            .setFailoverTimeout(configuration.getFailoverTimeout());

        FrameworkID frameworkID = configuration.getFrameworkId();
        if (!StringUtils.isEmpty(frameworkID.getValue())) {
            LOGGER.info("Found previous framework id: {}", frameworkID);
            frameworkBuilder.setId(frameworkID);
        }

        LOGGER.info("Starting Logstash Framework: \n{}", frameworkBuilder);

        driver = new MesosSchedulerDriver(this, frameworkBuilder.build(),
            configuration.getZookeeperUrl());

        driver.start();
    }

    @PreDestroy
    public void stop() throws ExecutionException, InterruptedException {
        configManager.setOnConfigUpdate(null);

        if (configuration.isDisableFailover()) {
            driver.stop(false);
            configuration.getFrameworkState().removeFrameworkId();
        } else {
          driver.stop(true);
        }
    }




    // Used by tests
    @SuppressWarnings("unused")
    public void registerListener(FrameworkMessageListener listener) {
        listeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(FrameworkMessageListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkId,
        MasterInfo masterInfo) {
        FrameworkState frameworkState = new FrameworkState(configuration.getState());
        frameworkState.setFrameworkId(frameworkId);
        configuration.setFrameworkState(frameworkState);

        LOGGER.info("Framework registered as: {}", frameworkId);

        ClusterState clusterState = new ClusterState(configuration.getState(), frameworkState);
        clusterMonitor = new ClusterMonitor(configuration, clusterState, liveState);
        statusUpdateWatchers.addObserver(clusterMonitor);
        List<Protos.Resource> resources = getResourcesList();

        Protos.Request request = Protos.Request.newBuilder()
            .addAllResources(resources)
            .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        schedulerDriver.requestResources(requests);

//        schedulerDriver.reconcileTasks(Collections.<Protos.TaskStatus>emptyList());
        registered = true;
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
        // TODO: reconcileTasks from persistent state
        // LOGGER.info("Re-registered framework: starting task reconciliation");
        // e.g. reconcileTasks(driver);

        LOGGER.info("Re-registered with master. ip={}", masterInfo.getId());
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
        if (!registered) {
            LOGGER.debug("Not registered, can't accept resource offers.");
            return;
        }
        offers.forEach(offer -> {

            // TODO: Debug log the offered resource,
            // it can be used to debug why executes are not spinning up.

            if (shouldAcceptOffer(offer)) {

                LOGGER.info("Accepting Offer. offerId={}, slaveId={}",
                    offer.getId().getValue(),
                    offer.getSlaveId());

                TaskInfo taskInfo = buildTask(offer);

                schedulerDriver.launchTasks(
                    singletonList(offer.getId()),
                    singletonList(taskInfo));

                clusterMonitor.monitorTask(taskInfo); // Add task to cluster monitor

            } else {

                schedulerDriver.declineOffer(offer.getId());
            }
        });
    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus status) {

        LOGGER.info("Received Status Update. taskId={}, state={}, message={}",
            status.getTaskId().getValue(),
            status.getState(),
            status.getMessage());

        statusUpdateWatchers.notifyObservers(status);

        if (isRunningState(status)) {
            // Send the executor the newest configuration.

            if (status.hasExecutorId()) {

                SchedulerMessage message = SchedulerMessage.newBuilder()
                    .setType(NEW_CONFIG)
                    .addAllConfigs(configManager.getLatestConfig())
                    .build();
                // TODO refactor into own statusUpdateWatcher
                sendMessage(status.getExecutorId(), status.getSlaveId(), message);

            } else {
                LOGGER.info("NO executor id passed: {}", status);
                TaskStatus taskStatus = TaskStatus.newBuilder().setTaskId(status.getTaskId()).setState(status.getState()).build();
                ArrayList<TaskStatus> statuses = new ArrayList<>();
                statuses.add(taskStatus);
                driver.reconcileTasks(statuses);
            }
        }
    }

    public void updateExecutorConfig(List<LogstashProtos.LogstashConfig> configs) {
        SchedulerMessage message = SchedulerMessage.newBuilder()
            .addAllConfigs(configs)
            .setType(NEW_CONFIG)
            .build();

        LOGGER.debug("Sending new config to all executors.");

        clusterMonitor.getRunningTasks().forEach(e ->
            sendMessage(e.getExecutorId(), e.getSlaveId(), message));
    }

    private void sendMessage(ExecutorID executorId, SlaveID slaveId, SchedulerMessage schedulerMessage) {

        LOGGER.debug("Sending message to executor {}", schedulerMessage);
        driver.sendFrameworkMessage(executorId, slaveId, schedulerMessage.toByteArray());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID,
        SlaveID slaveID, byte[] bytes) {

        try {
            ExecutorMessage message = ExecutorMessage.parseFrom(bytes);

            LOGGER.debug("Received Stats from Executor. executorId={}", executorID.getValue());
            message.getContainersList().forEach(container -> LOGGER.debug(container.toString()));

            liveState.updateStats(slaveID, message);

            listeners.forEach(l -> l.frameworkMessage(this, executorID, slaveID, message));

        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Failed to parse framework message. executorId={}, slaveId={}", executorID,
                slaveID, e);
        }
    }

    private String formatTaskId(Offer offer) {
        String date = new SimpleDateFormat(LogstashConstants.TASK_DATE_FORMAT).format(clock.now());
        return LogstashConstants.FRAMEWORK_NAME + "_" + offer.getHostname() + "_" + date;
    }

    private TaskInfo buildTask(Offer offer) {

        DockerInfo.Builder dockerExecutor = DockerInfo.newBuilder()
            .setForcePullImage(false)
            .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG);

        ContainerInfo.Builder container = ContainerInfo.newBuilder()
            .setType(Type.DOCKER)
            .setDocker(dockerExecutor.build());

        ExecutorEnvironmentalVariables executorEnvVars = new ExecutorEnvironmentalVariables(configuration);

        ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
            .setName(LogstashConstants.NODE_NAME + " executor")
            .setExecutorId(ExecutorID.newBuilder().setValue("executor." + UUID.randomUUID()))
            .setContainer(container)
            .setCommand(CommandInfo.newBuilder()
                .addArguments("dummyArgument")
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder()
                    .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG).build())
                .setEnvironment(Protos.Environment.newBuilder()
                    .addAllVariables(executorEnvVars.getList()))
                .setShell(false))
            .build();

        return TaskInfo.newBuilder()
            .setExecutor(executorInfo)
            .addAllResources(getResourcesList())
            .setName(LogstashConstants.TASK_NAME)
            .setTaskId(TaskID.newBuilder().setValue(formatTaskId(offer)))
                // TODO (thb) Consider using setData to pass the current config.
                // This would prevent a round trip, asking the scheduler for it.
                // e.g. .setData(latestConfig.toByteString())
            .setSlaveId(offer.getSlaveId())
            .build();
    }

    private List<Resource> getResourcesList() {

        int memNeeded = configuration.getExecutorHeapSize() + configuration.getLogstashHeapSize();

        return Arrays.asList(
            Resource.newBuilder()
                .setName("cpus")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder()
                    .setValue(configuration.getExecutorCpus()).build())
                .build(),
            Resource.newBuilder()
                .setName("mem")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder()
                    .setValue(memNeeded).build())
                .build()
        );
    }

    private boolean shouldAcceptOffer(Offer offer) {

        if (isHostAlreadyRunningTask(offer)){
            LOGGER.debug("Declined offer: Host " + offer.getHostname()
                + " is already running an Logstash task");
            return false;
        }

        boolean enoughCPU = hasEnoughOfResourceType(offer, "cpus", configuration.getExecutorCpus());
        boolean enoughMEM = hasEnoughOfResourceType(offer, "mem", configuration.getExecutorHeapSize() + configuration.getLogstashHeapSize());

        if (!enoughCPU) {
            LOGGER.debug("Declined offer: Not enough CPU resources");
            return false;
        } else if (!enoughMEM) {
            LOGGER.debug("Declined offer: Not enough MEM resources");
            return false;
        }

        return true;
    }

    private boolean hasEnoughOfResourceType(Offer offer, String resourceName, double minSize) {

        for (Resource resource : offer.getResourcesList()) {
            if (resourceName.equals(resource.getName())) {
                return resource.getScalar().getValue() >= minSize;
            }
        }

        return false;
    }

    public void requestExecutorStats() {

        SchedulerMessage message = SchedulerMessage.newBuilder()
            .setType(SchedulerMessage.SchedulerMessageType.REQUEST_STATS)
            .build();

        clusterMonitor.getRunningTasks().forEach(e ->
            sendMessage(e.getExecutorId(), e.getSlaveId(), message));
    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        LOGGER.warn("Scheduler driver disconnected.");
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String errorMessage) {
        LOGGER.error("Scheduler driver error. message={}", errorMessage);
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
        LOGGER.info("Offer Rescinded. offerId={}", offerID.getValue());
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
        LOGGER.info("Slave Lost. slaveId={}", slaveID.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID,
        SlaveID slaveID, int exitStatus) {
        // This is handled in statusUpdate.
    }

    private boolean isTerminalState(TaskStatus taskStatus) {
        return taskStatus.getState().equals(TaskState.TASK_FAILED)
            || taskStatus.getState().equals(TaskState.TASK_FINISHED)
            || taskStatus.getState().equals(TaskState.TASK_KILLED)
            || taskStatus.getState().equals(TaskState.TASK_LOST)
            || taskStatus.getState().equals(TaskState.TASK_ERROR);
    }

    private boolean isRunningState(TaskStatus taskStatus) {
        return taskStatus.getState().equals(TaskState.TASK_RUNNING);
    }


    private boolean isHostAlreadyRunningTask(Protos.Offer offer) {
        Boolean result = false;
        List<Protos.TaskInfo> stateList = clusterMonitor.getClusterState().getTaskList();
        for (Protos.TaskInfo t : stateList) {
            if (t.getSlaveId().equals(offer.getSlaveId())) {
                result = true;
            }
        }
        return result;
    }

    /**
     * Implementation of Observable to fix the setChanged problem.
     */
    private static class StatusUpdateObservable extends Observable {
        @Override
        public void notifyObservers(Object arg) {
            this.setChanged(); // This is ridiculous.
            super.notifyObservers(arg);
        }
    }
}
