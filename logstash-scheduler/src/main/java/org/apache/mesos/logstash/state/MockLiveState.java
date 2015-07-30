package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage.ExecutorStatus;
import org.apache.mesos.logstash.scheduler.Task;

import java.util.*;

import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.NOT_STREAMING;
import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.STREAMING;

/**
 * Mock Live State for use while developing the UI without having to run against
 * a live Mesos Cluster.
 */
public class MockLiveState implements ILiveState {

    private List<LogstashProtos.ContainerState> getRandomContainers() {
        List<LogstashProtos.ContainerState> containers = new ArrayList<>();

        containers.add(LogstashProtos.ContainerState.newBuilder()
            .setType(STREAMING)
            .setImageName("nginx")
            .setContainerId("1231243")
            .build());

        containers.add(LogstashProtos.ContainerState.newBuilder()
            .setType(STREAMING)
            .setImageName("website")
            .setContainerId("2231243")
            .build());

        containers.add(LogstashProtos.ContainerState.newBuilder()
            .setType(NOT_STREAMING)
            .setImageName("hadoop")
            .setContainerId("3231243")
            .build());

        return containers;
    }

    private ExecutorStatus getRandomStatus() {
        return ExecutorStatus.values()[new Random().nextInt(ExecutorStatus.values().length)];
    }

    private Task randomTask(String taskId, String slaveId, String executorId) {
        Task task = new Task(new Task(
            Protos.TaskID.newBuilder().setValue(taskId).build(),
            Protos.SlaveID.newBuilder().setValue(slaveId).build(),
            Protos.ExecutorID.newBuilder().setValue(executorId).build()
        ),
            getRandomContainers(),
            getRandomStatus());

        task.setStatus(Protos.TaskState.TASK_RUNNING);

        return task;
    }

    @Override
    public Set<Task> getTasks() {
        Set<Task> tasks = new HashSet<>();
        Random random = new Random();

        tasks.add(randomTask("TaskA", "6127-6213-7812-6312", "1221-1221-64-4345-4"));

        if (random.nextBoolean()) {
            tasks.add(randomTask("TaskB", "5555-6213-7812-1275", "9183-7221-24-3345-1"));
        }

        if (random.nextBoolean()) {
            tasks.add(randomTask("TaskC", "1112-3112-8463-1273", "4113-6222-22-3335-3"));
        }

        return tasks;
    }

    @Override
    public void removeRunningTask(Protos.SlaveID slaveId) {

    }

    @Override
    public void addRunningTask(Task task) {

    }

    @Override
    public void updateStats(Protos.SlaveID slaveId, LogstashProtos.ExecutorMessage message) {

    }
}
