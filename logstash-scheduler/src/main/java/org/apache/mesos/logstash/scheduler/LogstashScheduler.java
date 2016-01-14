package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.config.*;
import org.apache.mesos.logstash.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Component
public class LogstashScheduler implements org.apache.mesos.Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashScheduler.class);

    @Inject
    ConfigManager configManager;
    private final Collection<FrameworkMessageListener> listeners = synchronizedCollection(new ArrayList<>());

    @Inject
    Features features;
    @Inject
    FrameworkConfig frameworkConfig;

    @Inject
    TaskInfoBuilder taskInfoBuilder;

    private SchedulerDriver driver;

    @Inject
    MesosSchedulerDriverFactory mesosSchedulerDriverFactory;

    @Inject
    OfferStrategy offerStrategy;

    @Inject
    ClusterState clusterState;

    @Inject
    private FrameworkState frameworkState;

    @PostConstruct
    public void start() {
        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(frameworkConfig.getFrameworkName())
            .setUser(frameworkConfig.getUser())
            .setRole(frameworkConfig.getRole())
            .setCheckpoint(true)
            .setFailoverTimeout(frameworkConfig.getFailoverTimeout())
            .setId(frameworkState.getFrameworkID());

        LOGGER.info("Starting Logstash Framework: \n{}", frameworkBuilder);

        driver = mesosSchedulerDriverFactory.createMesosDriver(this, frameworkBuilder.build(),
            frameworkConfig.getZkUrl());

        driver.start();
    }

    @PreDestroy
    public void stop() throws ExecutionException, InterruptedException {
        configManager.setOnConfigUpdate(null);

        if (features.isFailover()) {
            driver.stop(true);
        } else {
            driver.stop(false);
            frameworkState.removeFrameworkId();
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

        frameworkState.setFrameworkId(frameworkId);

        LOGGER.info("Framework registered as: {}", frameworkId);

        Protos.Request request = Protos.Request.newBuilder()
            .addAllResources(taskInfoBuilder.getResourcesList())
            .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        schedulerDriver.requestResources(requests);
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
        LOGGER.info("Re-registered with master. ip={}", masterInfo.getId());
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
        offers.forEach(offer -> {
            if (shouldAcceptOffer(offer)) {

                LOGGER.info("Accepting Offer. offerId={}, slaveId={}",
                        offer.getId().getValue(),
                        offer.getSlaveId());

                TaskInfo taskInfo = taskInfoBuilder.buildTask(offer);

                schedulerDriver.launchTasks(
                        singletonList(offer.getId()),
                        singletonList(taskInfo));

                clusterState.addTask(taskInfo);
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
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID,
        SlaveID slaveID, byte[] bytes) {

        try {
            ExecutorMessage message = ExecutorMessage.parseFrom(bytes);

            LOGGER.debug("Received Stats from Executor. executorId={}", executorID.getValue());
            message.getContainersList().forEach(container -> LOGGER.debug(container.toString()));

            listeners.forEach(l -> l.frameworkMessage(this, executorID, slaveID, message));

        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Failed to parse framework message. executorId={}, slaveId={}", executorID,
                slaveID, e);
        }
    }

    private boolean shouldAcceptOffer(Offer offer) {
        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, offer);
        if (!result.acceptable) {
            LOGGER.debug("Declined offer: " + flattenProtobufString(offer.toString()) + " reason: " + result.reason.orElse("Unknown"));
        }
        return result.acceptable;
    }

    private String flattenProtobufString(String s) {
        return s.replace("  ", " ").replace("{\n", "{").replace("\n}", " }").replace("\n", ", ");
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
