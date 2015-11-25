package org.apache.mesos.logstash.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.state.LiveState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage.SchedulerMessageType.REQUEST_STATS;

/**
 * Executor for Logstash.
 */
public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashExecutor.class.toString());

    private final LogstashService logstashService;
    private final LiveState liveState;
    private final DockerClient dockerClient;

    public LogstashExecutor(LogstashService logstashService, DockerClient dockerClient, LiveState liveState) {
        this.logstashService = logstashService;
        this.dockerClient = dockerClient;
        this.liveState = liveState;
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        // FIXME for forwards compatibility, task.getData() this should be some data structure serialized in some extensible format e.g. JSON
        String elasticsearchDomainAndPort = task.getData().toStringUtf8();

        logstashService.update(514, elasticsearchDomainAndPort);
        logstashService.start();

        LOGGER.info("Notifying scheduler that executor has started.");

        driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
            .setExecutorId(task.getExecutor().getExecutorId())
            .setTaskId(task.getTaskId())
            .setState(Protos.TaskState.TASK_RUNNING).build());
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task. taskId={}", taskId.getValue());

        driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
            .setTaskId(taskId)
            .setState(Protos.TaskState.TASK_KILLED).build());

        driver.stop();
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {

        try {
            SchedulerMessage message = SchedulerMessage.parseFrom(data);

            LOGGER.info("SchedulerMessage. message={}", message);

            if (message.getType().equals(REQUEST_STATS)) {
                sendStatsToScheduler(driver);
            } else {
                updateConfig(message);
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error parsing framework message from scheduler.", e);
        }
    }

    private void updateConfig(SchedulerMessage message) {
        LOGGER.info("New configuration received. Reconfiguring...");

        // TODO extract config and update service:
        // logstashService.update(514, "elasticsearch.service:9200");
    }

    private void sendStatsToScheduler(ExecutorDriver driver) {
        driver.sendFrameworkMessage(liveState.getStateAsExecutorMessage().toByteArray());

    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        // The task i killed automatically, so we don't have to
        // do anything.
        LOGGER.info("Shutting down framework.");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: message={}", message);
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo,
        Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("LogstashExecutor Logstash registered. slaveId={}", slaveInfo.getId());

        liveState.setHostName(slaveInfo.getHostname());
        dockerClient.startupComplete(slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("LogstashExecutor Logstash re-registered. slaveId={}", slaveInfo.getId());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("LogstashExecutor Logstash disconnected");
    }
}
