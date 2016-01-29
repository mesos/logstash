package org.apache.mesos.logstash.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;

/**
 * Executor for Logstash.
 */
public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashExecutor.class);

    private final LogstashService logstashService;

    public LogstashExecutor(LogstashService logstashService) {
        this.logstashService = logstashService;
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        LOGGER.info("Launching task taskId={}", task.getTaskId());

        LogstashProtos.LogstashConfiguration logstashConfiguration;
        try {
            logstashConfiguration = LogstashProtos.LogstashConfiguration.parseFrom(task.getData().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        final Thread thread = new Thread(() -> {
            LOGGER.info("Forked thread with LogstashService.run()");
            try {
                logstashService.run(logstashConfiguration);
                LOGGER.info("LogstashService finished");
                driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
                        .setExecutorId(task.getExecutor().getExecutorId())
                        .setTaskId(task.getTaskId())
                        .setState(Protos.TaskState.TASK_FINISHED).build());
            } catch (Exception e) {
                LOGGER.error("Logstash service failed", e);
                driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
                        .setExecutorId(task.getExecutor().getExecutorId())
                        .setTaskId(task.getTaskId())
                        .setState(Protos.TaskState.TASK_FAILED)
                        .setMessage(e.getCause().getMessage()).build());
            }
            driver.stop();
        });
        thread.setDaemon(true);
        thread.start();

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
        LOGGER.info("LogstashExecutor.frameworkMessage");
        try {
            SchedulerMessage message = SchedulerMessage.parseFrom(data);
            LOGGER.info("SchedulerMessage. message={}", message);
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error parsing framework message from scheduler.", e);
        }
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
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
