package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ContainerInfo.Type;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.ConcurrentUtils;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.util.Clock;
import org.apache.mesos.logstash.util.LogstashConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedCollection;
import static org.apache.mesos.Protos.*;

@Component
public class LogstashScheduler implements org.apache.mesos.Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashScheduler.class);

    private final LiveState liveState;
    private final boolean isNoCluster;
    private final Collection<FrameworkMessageListener> listeners;

    private final Clock clock;

    private SchedulerMessage latestConfig;

    private final ExecutorService executorService;
    private final MesosSchedulerDriver driver;

    @Autowired
    public LogstashScheduler(
        LiveState liveState,
        @Qualifier("masterURL") String masterURL,
        @Qualifier("isNoCluster") boolean isNoCluster) {

        this.liveState = liveState;

        this.isNoCluster = isNoCluster;

        this.listeners = synchronizedCollection(new ArrayList<>());
        this.clock = new Clock();

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(LogstashConstants.FRAMEWORK_NAME)
            .setUser("root") // TODO: We should read this from the config.
            .setCheckpoint(true)
            .setFailoverTimeout(LogstashConstants.FAILOVER_TIMEOUT);

        driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), masterURL);
        executorService = Executors.newSingleThreadExecutor();
    }

    @PostConstruct
    public void start() {
        if (!isNoCluster) {
            executorService.execute(driver::run);
        }
    }

    @PreDestroy
    public void stop() {
        ConcurrentUtils.stop(executorService, 30);
    }

    public void registerListener(FrameworkMessageListener listener) {
        listeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(FrameworkMessageListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID,
        MasterInfo masterInfo) {
        // FIXME: We are required to persist this between runs for DCOS.
        // persistentState.setFrameworkId(frameworkID);

        // TODO: reconcile tasks upon registration
        // reconcileTasks(driver);
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
            LOGGER.info("Received Offer. offerId={}, slaveId={}",
                offer.getId().getValue(),
                offer.getSlaveId());

            // TODO: Debug log the offered resource,
            // it can be used to debug why executes are not spinning up.

            if (shouldAcceptOffer(offer)) {
                LOGGER.info("Accepting Offer.");
                schedulerDriver.launchTasks(
                    singletonList(offer.getId()),
                    singletonList(buildTask(offer)));
            } else {
                LOGGER.info("Declining Offer.");
                schedulerDriver.declineOffer(offer.getId());
            }
        });
    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus status) {

        LOGGER.info("Received Status Update. slaveId={}, executorId={}, state={}",
            status.getTaskId().getValue(),
            status.getState().toString(),
            status.getMessage());

        if (isRunningState(status)) {
            // Send the executor the newest configuration.
            if (latestConfig != null) {
                sendMessage(status.getExecutorId(), status.getSlaveId(), latestConfig);
            }
            // TODO (thb) discuss this. How should we ensure the order configs are sent.
            // We need to prevent tasks receiving old configs because of race condition.
            liveState.addRunningTask(new Task(status.getTaskId(), status.getSlaveId(), status.getExecutorId()));
        } else if (isTerminalState(status)) {
            liveState.removeRunningTask(status.getSlaveId());
        } else {
            LOGGER.debug("No action required after status update. state={}", status.getState());
        }
    }

    public void configUpdated(Map<String, String> dockerConfig, Map<String, String> hostConfig) {
        SchedulerMessage.Builder builder = SchedulerMessage.newBuilder();

        for (Map.Entry<String, String> entry : dockerConfig.entrySet()) {
            builder.addDockerConfig(LogstashProtos.LogstashConfig.newBuilder()
                .setConfig(entry.getValue())
                .setFrameworkName(entry.getKey()));
        }

        for (Map.Entry<String, String> entry : hostConfig.entrySet()) {
            builder.addHostConfig(LogstashProtos.LogstashConfig.newBuilder()
                .setConfig(entry.getValue())
                .setFrameworkName(entry.getKey()));
        }

        SchedulerMessage message = this.latestConfig = builder.build();
        liveState.getTasks().forEach(e ->
            sendMessage(e.getExecutorID(), e.getSlaveID(), message));
    }

    private void sendMessage(ExecutorID executorId, SlaveID slaveId, SchedulerMessage config) {
        driver.sendFrameworkMessage(executorId, slaveId, config.toByteArray());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID,
        SlaveID slaveID, byte[] bytes) {

        try {
            ExecutorMessage message = ExecutorMessage.parseFrom(bytes);
            LOGGER.debug("Received Framework Message. type={}, state={}", message.getType(),
                message.getGlobalStateInfo());
            liveState.getTasks();
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

        ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
            .setName(LogstashConstants.NODE_NAME + " executor")
            .setExecutorId(ExecutorID.newBuilder().setValue("executor." + UUID.randomUUID()))
            .addAllResources(getResourcesList())
            .setContainer(container)
            .setCommand(CommandInfo.newBuilder()
                .addArguments("java")
                .addArguments("-Djava.library.path=/usr/local/lib")
                .addArguments("-jar")
                .addArguments("/tmp/logstash-executor.jar")
                .setShell(false))
            .build();

        return TaskInfo.newBuilder()
            .setExecutor(executorInfo)
            .setName(LogstashConstants.TASK_NAME)
            .setTaskId(TaskID.newBuilder().setValue(formatTaskId(offer)))
            // TODO (thb) Consider using setData to pass the current config.
            // This would prevent a round trip, asking the scheduler for it.
            // e.g. .setData(latestConfig.toByteString())
            .setSlaveId(offer.getSlaveId())
            .build();
    }

    private List<Resource> getResourcesList() {

        return Arrays.asList(
            // FIXME: Read these numbers from the commandline.
            Resources.cpus(LOGSTASH_CPU_DEFAULT),
            Resources.mem(LOGSTASH_MEMORY_DEFAULT),
            Resources.disk(LOGSTASH_DISK_DEFAULT)
        );
    }

    private boolean shouldAcceptOffer(Offer offer) {
        // Don't run the same framework multiple times on the same host

        // FIXME (thb): What if we never actually manage to get an executor running on
        // this host or it fails after a while. We will never try again.

        boolean slaveHasTask = liveState.getTasks().stream().anyMatch(e ->
            e.getSlaveID().equals(offer.getSlaveId()));

        // FIXME: What if the offer does not contain enough resources?
        // e.g. only 32MB mem when we need 200MB
        return !slaveHasTask;
    }

    public void requestInternalStatus() {

        SchedulerMessage message = SchedulerMessage.newBuilder()
            .setCommand("REPORT_INTERNAL_STATUS")
            .build();

        liveState.getTasks().forEach(e ->
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
