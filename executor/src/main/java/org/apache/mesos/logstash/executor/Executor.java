package org.apache.mesos.logstash.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.LogConfigurationListener;
import org.apache.mesos.logstash.LogstashInfo;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;

/**
 * Executor for Logstash.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class Executor implements org.apache.mesos.Executor {

    public static final Logger LOGGER = Logger.getLogger(Executor.class.toString());

    private LogConfigurationListener listener = null;

    public Executor(LogConfigurationListener listener) {
        super();
        this.listener = listener;
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Logstash registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Logstash re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Logstash disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {

        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(status);

        try {
            assert listener != null;
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setState(Protos.TaskState.TASK_FINISHED).build();
                    driver.sendStatusUpdate(taskStatus);
                }
            }) {
            });
        } catch (Exception e) {
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_FAILED).build();
            driver.sendStatusUpdate(status);
        }
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_FAILED).build();
        driver.sendStatusUpdate(status);
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message: " + Arrays.toString(data));

        try {
            SchedulerMessage schedulerMessage = SchedulerMessage.parseFrom(data);

            Stream<LogstashInfo> dockerInfos = extractConfigs(schedulerMessage.getDockerConfigList().stream());
            Stream<LogstashInfo> hostInfos = extractConfigs(schedulerMessage.getHostConfigList().stream());

            listener.updatedDockerLogConfigurations(dockerInfos);
            listener.updatedHostLogConfigurations(hostInfos);

        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error parsing framework message from scheduler", e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
        }
    }

    private Stream<LogstashInfo> extractConfigs(Stream<LogstashConfig> cfgs) {
        return cfgs.map(cfg -> new LogstashInfo(cfg.getFrameworkName(), cfg.getConfig()));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down framework...");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
    }

}
