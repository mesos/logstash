package org.apache.mesos.logstash.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;
import org.apache.mesos.logstash.executor.state.GlobalStateInfo;

import java.util.stream.Stream;

import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;

public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = Logger.getLogger(LogstashExecutor.class.toString());

    private final ConfigEventListener listener;
    private final GlobalStateInfo globalStateInfo;
    private final StartupListener startupListener;

    public LogstashExecutor(ConfigEventListener listener, StartupListener startupListener, GlobalStateInfo globalStateInfo) {
        this.listener = listener;
        this.startupListener = startupListener;
        this.globalStateInfo = globalStateInfo;
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {

        LOGGER.info("Notifying scheduler that task has launched");
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setExecutorId(task.getExecutor().getExecutorId())
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(status);

        System.out.println("Task has been launched");

        // TODO clean up and make the purpose clear
        try {
            assert listener != null;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setState(Protos.TaskState.TASK_FINISHED).build();
                driver.sendStatusUpdate(taskStatus);
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

        LOGGER.info("Framework Message Received. Parsing contents.");

        try {
            SchedulerMessage schedulerMessage = SchedulerMessage.parseFrom(data);
            LOGGER.debug("SchedulerMessage:\n" + schedulerMessage);

            if (schedulerMessage.hasCommand()){ // currently we assume that commands
                handleCommand(driver,schedulerMessage);
            }

            Stream<FrameworkInfo> dockerInfos = extractConfigs(schedulerMessage.getDockerConfigList().stream());
            listener.onConfigUpdated(LogType.DOCKER, dockerInfos);

            Stream<FrameworkInfo> hostInfos = extractConfigs(schedulerMessage.getHostConfigList().stream());
            listener.onConfigUpdated(LogType.HOST, hostInfos);

            if (schedulerMessage.getDockerConfigList().size() > 0 || schedulerMessage.getHostConfigList().size() > 0){
                LOGGER.info("Logstash configuration updated.");
            }


        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error parsing framework message from scheduler.", e);
        }
    }

    private void handleCommand(ExecutorDriver driver, SchedulerMessage schedulerMessage) {
        LOGGER.info("Logstash received command: " + schedulerMessage.getCommand());
        if (schedulerMessage.getCommand().equals("REPORT_INTERNAL_STATUS")){
            driver.sendFrameworkMessage(globalStateInfo.getStateAsExecutorMessage().toByteArray());
        }
    }

    private Stream<FrameworkInfo> extractConfigs(Stream<LogstashConfig> cfgs) {
        return cfgs.map(cfg -> new FrameworkInfo(cfg.getFrameworkName(), cfg.getConfig()));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down framework...");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
    }


    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("LogstashExecutor Logstash registered on slave " + slaveInfo.getHostname());

        startupListener.startupComplete(slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("LogstashExecutor Logstash re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("LogstashExecutor Logstash disconnected");
    }
}
