package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.scheduler.Task;

import java.util.*;

public class MockLiveState implements LiveState {

    private List<LogstashProtos.ContainerState> getRandomContainers() {
        List<LogstashProtos.ContainerState> containers = new ArrayList<>();

        containers.add(LogstashProtos.ContainerState.newBuilder().setType(
            LogstashProtos.ContainerState.LoggingStateType.STREAMING).setName("nginx").build());

        containers.add(LogstashProtos.ContainerState.newBuilder().setType(
            LogstashProtos.ContainerState.LoggingStateType.STREAMING).setName("website").build());

        containers.add(LogstashProtos.ContainerState.newBuilder().setType(
            LogstashProtos.ContainerState.LoggingStateType.NOT_STREAMING).setName("hadoop")
            .build());

        return containers;
    }

    private Task randomTask(String taskId, String slaveId, String executorId) {
        Task task = new Task(new Task(
            Protos.TaskID.newBuilder().setValue(taskId).build(),
            Protos.SlaveID.newBuilder().setValue(slaveId).build(),
            Protos.ExecutorID.newBuilder().setValue(executorId).build()
        ),
            getRandomContainers());

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
    public void updateStats(Protos.SlaveID slaveId,
        List<LogstashProtos.ContainerState> containers) {

    }
}
