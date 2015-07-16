package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import org.apache.mesos.logstash.scheduler.ui.Executor;
import org.apache.mesos.logstash.scheduler.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Collections.synchronizedCollection;
import static java.util.Collections.synchronizedSet;
import static org.apache.mesos.Protos.TaskState.TASK_FINISHED;
import static org.apache.mesos.Protos.TaskState.TASK_LOST;
import static org.apache.mesos.Protos.TaskState.TASK_RUNNING;

@Component
public class Scheduler implements org.apache.mesos.Scheduler, ConfigEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private static final String TASK_NAME = "LOGSTASH_SERVER";
    private static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    private final Driver driver;
    private final ConfigManager configManager;
    private final Set<Executor> executors;
    private final Collection<FrameworkMessageListener> listeners;

    private final Clock clock;
    private final Set<Task> tasks;

    private Protos.FrameworkID frameworkId;


    @Autowired
    public Scheduler(Driver driver, ConfigManager configManager) {
        this.driver = driver;
        this.configManager = configManager;
        this.executors = synchronizedSet(new HashSet<>());
        this.listeners = synchronizedCollection(new ArrayList<>());
        clock = new Clock();
        tasks = new HashSet<>();
    }


    @PostConstruct
    public void start() {
        configManager.registerListener(this);
    }


    public void registerListener(FrameworkMessageListener listener) {
        listeners.add(listener);
    }


    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        // FIXME: We are required to persist this between runs for DCOS.
        this.frameworkId = frameworkID;
    }


    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {
        for (Protos.Offer offer : list) {
            LOGGER.debug("Received offer from slave " + offer.getSlaveId() + ", " + offer.getHostname());

            if (shouldAcceptOffer(offer)) {
                LOGGER.info("Accepting Offer. id=" + offer.getId());

                String id = formatTaskId(offer);
                Protos.TaskInfo taskInfo = buildTask(offer, id);

                tasks.add(new Task(offer.getHostname(), id));
                schedulerDriver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
            } else {
                LOGGER.info("Declining Offer. id=" + offer.getId());
                schedulerDriver.declineOffer(offer.getId());
            }
        }
    }


    @Override
    public synchronized void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus status) {
        LOGGER.info("Task status update! " + status.toString());

        if (status.getState() == TASK_RUNNING) {

            LOGGER.info("Executor Started. slaveId=" + status.getSlaveId() + ", executorId=" + status.getExecutorId());

            // Add the executor to the set of active executors.
            executors.add(Executor.fromTaskStatus(status));

            // Send it the newest configuration.
            byte[] config = marshallConfig(configManager.getConfig());
            sendMessage(Executor.fromTaskStatus(status), config);
        } else if (status.getState() == TASK_FINISHED || status.getState() == TASK_LOST) {

            LOGGER.info("Executor Removed. slaveId=" + status.getSlaveId() + ", executorId=" + status.getExecutorId());

            // Remove the executor from the set of active executors.
            executors.remove(Executor.fromTaskStatus(status));
        }
    }


    @Override
    public void configUpdated(ConfigManager.ConfigPair config) {
        // Configs have changed. Notify all executors.
        byte[] message = marshallConfig(config);
        executors.stream().forEach(executor -> sendMessage(executor, message));
    }


    private byte[] marshallConfig(ConfigManager.ConfigPair config) {
        LogstashProtos.SchedulerMessage.Builder builder = LogstashProtos.SchedulerMessage.newBuilder();

        for (Map.Entry<String, String> entry : config.getDockerConfig().entrySet()) {
            builder.addDockerConfig(LogstashProtos.LogstashConfig.newBuilder()
                    .setConfig(entry.getValue())
                    .setFrameworkName(entry.getKey()));
        }

        for (Map.Entry<String, String> entry : config.getHostConfig().entrySet()) {
            builder.addHostConfig(LogstashProtos.LogstashConfig.newBuilder()
                    .setConfig(entry.getValue())
                    .setFrameworkName(entry.getKey()));
        }

        return builder.build().toByteArray();
    }


    private void sendMessage(Executor executor, byte[] config) {
        driver.sendFrameworkMessage(executor.getExecutorID(), executor.getSlaveID(), config);
    }


    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, byte[] bytes) {

        Executor executor = new Executor(slaveID, executorID);
        ExecutorMessage message;

        try {
            message = ExecutorMessage.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Failed to parse framework message.", e);
            return;
        }

        LOGGER.info("Received Framework Message: type=" + message.getType() + ", content='" + (message.hasGlobalStateInfo() ? message.getGlobalStateInfo() : "") + "'");
        listeners.stream().forEach(l -> l.frameworkMessage(executor, message));
    }


    private String formatTaskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format(Constants.FRAMEWORK_NAME + "_%s_%s", offer.getHostname(), date);
    }


    private Protos.TaskInfo buildTask(Protos.Offer offer, String id) {

        LOGGER.info("Building Task");

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(offer.getSlaveId())

                // FIXME: Only accept the recources we need.
                .addAllResources(offer.getResourcesList());

        Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo.newBuilder()
                .setForcePullImage(false)
                .setImage("mesos/logstash-executor");


        LOGGER.info("Using Executor to run Logstash cloud mesos on slaves");

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(frameworkId)
                .setContainer(Protos.ContainerInfo.newBuilder().setType(Protos.ContainerInfo.Type.DOCKER).setDocker(dockerExecutor.build()))
                .setName("Logstash_" + UUID.randomUUID())
                // TODO verify that this command is actually the one being used (as opposed to the one specified in the docker file)
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
        // Don't run the same framework multiple times on the same host

        // FIXME (thb): What if we never actually manage to get an executor running on
        // this host or it fails after a while. We will never try again.
        for (Task task : tasks) {
            if (task.getHostname().equals(offer.getHostname())) {
                return false;
            }
        }

        // FIXME: What if the offer does not contain enough resources?
        // e.g. only 32MB mem when we need 200MB
        return true;
    }


    public void requestInternalStatus() {

        byte[] message = SchedulerMessage.newBuilder()
                .setCommand("REPORT_INTERNAL_STATUS")
                .build()
                .toByteArray();

        executors.stream().forEach(executor -> sendMessage(executor, message));
    }


    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Re-registered with master. ip=" + masterInfo.getId());
    }


    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        LOGGER.warn("Disconnected from master.");
    }


    @Override
    public void error(SchedulerDriver schedulerDriver, String errorMessage) {
        LOGGER.error("Cluster Error. " + errorMessage);
    }


    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        LOGGER.info("Offer Rescinded");
    }


    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
    }


    @Override
    public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int exitStatus) {
    }
}
