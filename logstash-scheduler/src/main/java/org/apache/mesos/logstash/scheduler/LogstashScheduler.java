package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.cluster.ClusterMonitor;
import org.apache.mesos.logstash.cluster.ClusterMonitor.ExecutionPhase;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import org.apache.mesos.logstash.config.*;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.LSTaskStatus;
import org.apache.mesos.logstash.state.LiveState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedCollection;
import static org.apache.mesos.Protos.*;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage.SchedulerMessageType.NEW_CONFIG;

@Component
public class LogstashScheduler implements org.apache.mesos.Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashScheduler.class);

    @Inject
    ConfigManager configManager;
    private final Collection<FrameworkMessageListener> listeners = synchronizedCollection(new ArrayList<>());

    @Inject
    LiveState liveState;

    @Inject
    Configuration configuration;

    @Inject
    FrameworkConfig frameworkConfig;

    @Inject
    TaskInfoBuilder taskInfoBuilder;

    private SchedulerDriver driver;
    ClusterMonitor clusterMonitor = null;
    private Observable statusUpdateWatchers = new StatusUpdateObservable();

    @Inject
    MesosSchedulerDriverFactory mesosSchedulerDriverFactory;

    @Inject
    OfferStrategy offerStrategy;

    @PostConstruct
    public void start() {
        configManager.setOnConfigUpdate(this::updateExecutorConfig);

        String webUiURL = createWebuiUrl(configuration.getWebServerPort());

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(frameworkConfig.getFrameworkName())
            .setUser(configuration.getLogStashUser())
            .setRole(configuration.getLogStashRole())
            .setCheckpoint(true)
            .setFailoverTimeout(configuration.getFailoverTimeout());

        if (webUiURL != null) {
            frameworkBuilder.setWebuiUrl(createWebuiUrl(configuration.getWebServerPort()));
        }

        FrameworkID frameworkID = configuration.getFrameworkId();
        if (!StringUtils.isEmpty(frameworkID.getValue())) {
            LOGGER.info("Found previous framework id: {}", frameworkID);
            frameworkBuilder.setId(frameworkID);
        }

        LOGGER.info("Starting Logstash Framework: \n{}", frameworkBuilder);

        driver = mesosSchedulerDriverFactory.createMesosDriver(this, frameworkBuilder.build(),
            frameworkConfig.getZkUrl());

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

        Protos.Request request = Protos.Request.newBuilder()
            .addAllResources(taskInfoBuilder.getResourcesList())
            .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        schedulerDriver.requestResources(requests);

        reconcileTasks();
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
        LOGGER.info("Re-registered with master. ip={}", masterInfo.getId());
        reconcileTasks();
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
        if(clusterMonitor.isReconciling()) {
            LOGGER.debug("Still in the reconciliation phase, can't accept resource offers.");
            offers.forEach(offer -> driver.declineOffer(offer.getId()));
            return;
        }

        offers.forEach(offer -> {
            if (shouldAcceptOffer(offer)) {

                LOGGER.info("Accepting Offer. offerId={}, slaveId={}",
                        offer.getId().getValue(),
                        offer.getSlaveId());

                TaskInfo taskInfo = taskInfoBuilder.buildTask(offer);

                schedulerDriver.launchTasks(
                        singletonList(offer.getId()),
                        singletonList(taskInfo));

                clusterMonitor.monitorTask(taskInfo); // Add task to cluster monitor

                // Store a fingerprint so we can figure out when the configuration has changed
                clusterMonitor.getClusterState().getStatus(taskInfo.getTaskId())
                        .setConfigurationFingerprint(configuration.getFingerprint());

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

            LSTaskStatus lsTaskStatus = clusterMonitor
                .getClusterState().getStatus(status.getTaskId());

            if(lsTaskStatus.getConfigurationFingerprint().equals(configuration.getFingerprint())) {
                SchedulerMessage message = SchedulerMessage.newBuilder()
                        .setType(NEW_CONFIG)
                        .addAllConfigs(configManager.getLatestConfig())
                        .build();
                // TODO refactor into own statusUpdateWatcher
                sendMessage(lsTaskStatus.getTaskInfo().getExecutor().getExecutorId(), lsTaskStatus.getTaskInfo().getSlaveId(), message);

            }
            else {
                driver.killTask(status.getTaskId());
            }
        }

        schedulerDriver.reviveOffers();
    }

    public void updateExecutorConfig(List<LogstashProtos.LogstashConfig> configs) {

        if (clusterMonitor.getExecutionPhase() == ExecutionPhase.RECONCILING_TASKS){
            return; // we will update the config as soon as we get a status update
        }

        SchedulerMessage message = SchedulerMessage.newBuilder()
            .addAllConfigs(configs)
            .setType(NEW_CONFIG)
            .build();

        LOGGER.debug("Sending new config to all executors.");

        clusterMonitor.getRunningTasks().forEach(e ->
            sendMessage(e.getExecutor().getExecutorId(), e.getSlaveId(), message));
    }

    private void sendMessage(ExecutorID executorId, SlaveID slaveId,
        SchedulerMessage schedulerMessage) {

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

    private boolean shouldAcceptOffer(Offer offer) {
        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterMonitor.getClusterState(), offer);
        if (!result.acceptable) {
            LOGGER.debug("Declined offer: " + flattenProtobufString(offer.toString()) + " reason: " + result.reason.orElse("Unknown"));
        }
        return result.acceptable;
    }

    private String flattenProtobufString(String s) {
        return s.replace("  ", " ").replace("{\n", "{").replace("\n}", " }").replace("\n", ", ");
    }

    public void requestExecutorStats() {

        if (clusterMonitor == null || clusterMonitor.getExecutionPhase() == ExecutionPhase.RECONCILING_TASKS){
            LOGGER.debug("Supress requesting executor stats, because we're still setting up...");
            return;

        }

        SchedulerMessage message = SchedulerMessage.newBuilder()
            .setType(SchedulerMessage.SchedulerMessageType.REQUEST_STATS)
            .build();

        List<TaskInfo> runningTasks = clusterMonitor.getRunningTasks();

        if(runningTasks.isEmpty()) {
            LOGGER.warn("No tasks to request stats from");
        }
        else {
            runningTasks.forEach(e -> {
                try {
                    sendMessage(e.getExecutor().getExecutorId(), e.getSlaveId(), message);
                } catch (Exception ex) {
                    LOGGER.error("Can not send message: {}", ex);
                }
            });
        }
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

        schedulerDriver.reviveOffers();
    }

    private boolean isRunningState(TaskStatus taskStatus) {
        return taskStatus.getState().equals(TaskState.TASK_RUNNING);
    }


    private void reconcileTasks() {
        clusterMonitor.startReconciling(driver);
    }


    private String createWebuiUrl(int webServerPort) {
        String webUiUrl = null;
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            webUiUrl = "http:\\/\\/" + hostName + ":" + webServerPort;
        } catch (UnknownHostException e) {
            LOGGER.warn("Can not determine host name", e);
        }
        LOGGER.debug("Setting webuiUrl to " + webUiUrl);
        return webUiUrl;
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
