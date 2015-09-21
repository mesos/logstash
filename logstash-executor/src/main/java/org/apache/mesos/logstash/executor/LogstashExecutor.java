package org.apache.mesos.logstash.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.log4j.Logger;

/**
 * Executor for Logstash.
 */
public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = Logger.getLogger(LogstashExecutor.class.toString());
    private TaskStatus taskStatus;

    public LogstashExecutor(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info(String.format("LogstashExecutor Logstash registered. slaveId=%s", slaveInfo.getId()));
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info(String.format("LogstashExecutor Logstash re-registered. slaveId=%s", slaveInfo.getId()));
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("LogstashExecutor Logstash disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        LOGGER.info("Notifying scheduler that executor has started.");

        Protos.TaskID taskID = task.getTaskId();
        taskStatus.setTaskID(taskID);

        driver.sendStatusUpdate(taskStatus.running());
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info(String.format("Kill task. taskId=%s", taskId.getValue()));

        driver.sendStatusUpdate(taskStatus.killed());
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message");
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        // The task i killed automatically, so we don't have to
        // do anything.
        LOGGER.info("Shutting down framework.");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info(String.format("Error in executor: message=%s", message));
    }

}
