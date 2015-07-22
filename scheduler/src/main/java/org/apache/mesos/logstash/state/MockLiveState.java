package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.scheduler.Task;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class MockLiveState implements LiveState {

    private Task randomExecutor(String taskId, String slaveId, String executorId, int count) {
        Task task = new Task(
            Protos.TaskID.newBuilder().setValue(taskId).build(),
            Protos.SlaveID.newBuilder().setValue(slaveId).build(),
            Protos.ExecutorID.newBuilder().setValue(executorId).build()
        );

        task.setStatus(Protos.TaskState.TASK_RUNNING);

        task.setActiveStreamCount(count);

        return task;
    }

    @Override
    public Set<Task> getTasks() {
        Set<Task> tasks = new HashSet<>();
        Random random = new Random();

        tasks.add(
            randomExecutor("TaskA", "6127-6213-7812-6312", "1221-1221-64-4345-4",
                random.nextInt(20) + 10));

        if (random.nextBoolean()) {
            tasks.add(randomExecutor("TaskB", "5555-6213-7812-1275", "9183-7221-24-3345-1",
                random.nextInt(30) + 2));
        }

        if (random.nextBoolean()) {
            tasks.add(
                randomExecutor("TaskC", "1112-3112-8463-1273", "4113-6222-22-3335-3",
                    random.nextInt(7)));
        }

        return tasks;
    }

    @Override
    public void removeRunningTask(Protos.SlaveID slaveId) {

    }

    @Override
    public void addRunningTask(Task task) {

    }
}
