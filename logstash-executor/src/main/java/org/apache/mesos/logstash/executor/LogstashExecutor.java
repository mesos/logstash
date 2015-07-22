package org.apache.mesos.logstash.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogType;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.apache.mesos.logstash.executor.state.GlobalStateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage.SchedulerMessageType.REQUEST_STATS;

public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashExecutor.class.toString());

    private final ConfigEventListener listener;
    private final GlobalStateInfo globalStateInfo;
    private final StartupListener startupListener;

    public LogstashExecutor(ConfigEventListener listener, StartupListener startupListener,
        GlobalStateInfo globalStateInfo) {
        this.listener = listener;
        this.startupListener = startupListener;
        this.globalStateInfo = globalStateInfo;
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {

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
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {

        try {
            SchedulerMessage message = SchedulerMessage.parseFrom(data);

            LOGGER.info("SchedulerMessage. message={}", message);

            if (message.getType().equals(REQUEST_STATS)) {
                sendStatsToScheduler(driver, message);
            } else {
                updateConfig(message);
            }

        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error parsing framework message from scheduler.", e);
        }
    }

    private void updateConfig(SchedulerMessage message) {
        Stream<FrameworkInfo> dockerInfos = extractConfigs(
            message.getDockerConfigList().stream());

        Stream<FrameworkInfo> hostInfos = extractConfigs(
            message.getHostConfigList().stream());

        listener.onConfigUpdated(LogType.DOCKER, dockerInfos);
        listener.onConfigUpdated(LogType.HOST, hostInfos);

        if (message.getDockerConfigList().size() > 0
            || message.getHostConfigList().size() > 0) {
            LOGGER.info("Logstash configuration updated.");
        }
    }

    private void sendStatsToScheduler(ExecutorDriver driver, SchedulerMessage schedulerMessage) {
        driver.sendFrameworkMessage(globalStateInfo.getStateAsExecutorMessage().toByteArray());

    }

    private Stream<FrameworkInfo> extractConfigs(Stream<LogstashConfig> cfgs) {
        return cfgs.map(cfg -> new FrameworkInfo(cfg.getFrameworkName(), cfg.getConfig()));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        // The task i killed automatically.
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

        startupListener.startupComplete(slaveInfo.getHostname());
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
