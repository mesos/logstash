package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ContainerInfo.Type;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.ExecutorEnvironmentalVariables;
import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.logstash.state.ILiveState;
import org.apache.mesos.logstash.state.IPersistentState;
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

    private final ILiveState liveState;
    private final IPersistentState persistentState;
    private final ConfigManager configManager;
    private final LogstashSettings settings;
    private final Collection<FrameworkMessageListener> listeners;

    private final Clock clock;
    private MesosSchedulerDriver driver;

    @Autowired
    public LogstashScheduler(
        ILiveState liveState,
        IPersistentState persistentState,
        ConfigManager configManager,
        LogstashSettings settings) {

        this.liveState = liveState;
        this.persistentState = persistentState;
        this.configManager = configManager;
        this.settings = settings;

        this.listeners = synchronizedCollection(new ArrayList<>());
        this.clock = new Clock();
    }

    @PostConstruct
    public void start() {
        configManager.setOnConfigUpdate(this::updateExecutorConfig);

        if (!settings.getWebServerDebug()) {
            Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setName(settings.getFrameworkName())
                .setUser(settings.getLogstashUser())
                .setRole(settings.getLogstashRole())
                .setCheckpoint(true)
                .setFailoverTimeout(settings.getFailoverTimeout());

            try {
                FrameworkID frameworkID = persistentState.getFrameworkID();
                if (frameworkID != null) {
                    LOGGER.info("Found previous framework id: {}", frameworkID);
                    frameworkInfo.setId(frameworkID);
                }
            } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
                throw new SchedulerException("Error recovering framework id", e);
            }

            LOGGER.info("Starting Logstash Framework: \n{}", frameworkInfo);

            driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
                settings.getMesosMasterUri());

            driver.start();
        }
    }

    @PreDestroy
    public void stop() throws ExecutionException, InterruptedException {
        configManager.setOnConfigUpdate(null);

        if (!settings.getWebServerDebug()) {
            // We are doing a graceful shutdown so we should
            // remove our framework id, so we don't try to reconnect
            // on startup.
            persistentState.removeFrameworkId();
            driver.stop(false);
        }
    }

    public void fail() {
        if (!settings.getWebServerDebug()) {
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
        LOGGER.info("Framework registered as: {}",frameworkId);
        try {
            persistentState.setFrameworkId(frameworkId);
        } catch (InterruptedException | ExecutionException e) {
            // these are zk exceptions... we are unable to maintain state.
            throw new SchedulerException("Error setting framework id in persistent state", e);
            // FIXME: reconcileTasks(driver);
        }

        List<Protos.Resource> resources = getResourcesList();

        Protos.Request request = Protos.Request.newBuilder()
            .addAllResources(resources)
            .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        schedulerDriver.requestResources(requests);
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
        offers.forEach(offer -> {

            // TODO: Debug log the offered resource,
            // it can be used to debug why executes are not spinning up.

            if (shouldAcceptOffer(offer)) {

                LOGGER.info("Accepting Offer. offerId={}, slaveId={}",
                    offer.getId().getValue(),
                    offer.getSlaveId());

                schedulerDriver.launchTasks(
                    singletonList(offer.getId()),
                    singletonList(buildTask(offer)));

                liveState.addStagingTaskOnSlave(offer.getSlaveId());

            } else {
                LOGGER.debug("Declining Offer. offerId={}, slaveId={}",
                    offer.getId().getValue(),
                    offer.getSlaveId());
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

        if (isRunningState(status)) {
            // Send the executor the newest configuration.

            SchedulerMessage message = SchedulerMessage.newBuilder()
                .setType(NEW_CONFIG)
                .addAllConfigs(configManager.getLatestConfig())
                .build();

            sendMessage(status.getExecutorId(), status.getSlaveId(), message);

            // We need to prevent tasks receiving old configs because of race condition.
            liveState.addRunningTask(
                new Task(status.getTaskId(), status.getSlaveId(), status.getExecutorId()));
        } else if (isTerminalState(status)) {
            liveState.removeTask(status.getSlaveId());
        } else {
            LOGGER.debug("No action required after status update. state={}", status.getState());
        }
    }

    public void updateExecutorConfig(List<LogstashProtos.LogstashConfig> configs) {
        SchedulerMessage message = SchedulerMessage.newBuilder()
            .addAllConfigs(configs)
            .setType(NEW_CONFIG)
            .build();

        LOGGER.debug("Sending new config to all executors.");

        liveState.getRunningTasks().forEach(e ->
            sendMessage(e.getExecutorID(), e.getSlaveID(), message));
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
            .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME);

        ContainerInfo.Builder container = ContainerInfo.newBuilder()
            .setType(Type.DOCKER)
            .setDocker(dockerExecutor.build());

        ExecutorEnvironmentalVariables executorEnvVars = new ExecutorEnvironmentalVariables(settings);

        ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
            .setName(LogstashConstants.NODE_NAME + " executor")
            .setExecutorId(ExecutorID.newBuilder().setValue("executor." + UUID.randomUUID()))
            .setContainer(container)
            .setCommand(CommandInfo.newBuilder()
                .addArguments("dummyArgument")
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder()
                    .setImage(LogstashConstants.EXECUTOR_IMAGE_NAME).build())
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

        int memNeeded = settings.getExecutorHeapSize() + settings.getLogstashHeapSize();

        return Arrays.asList(
            Resource.newBuilder()
                .setName("cpus")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder()
                    .setValue(settings.getExecutorCpus()).build())
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

        boolean enoughCPU = hasEnoughOfResourceType(offer, "cpus", settings.getExecutorCpus());
        boolean enoughMEM = hasEnoughOfResourceType(offer, "mem", settings.getExecutorHeapSize());

        if (!enoughCPU || !enoughMEM) {
            return false;
        }

        // Don't run the same framework multiple times on the same host
        if (liveState.isAlreadyStaging(offer.getSlaveId())){
            return false;
        }

        boolean slaveHasTask = liveState.getRunningTasks().stream().anyMatch(e ->
            e.getSlaveID().equals(offer.getSlaveId()));

        return !slaveHasTask;
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

        liveState.getRunningTasks().forEach(e ->
            sendMessage(e.getExecutorID(), e.getSlaveID(), message));
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
}
