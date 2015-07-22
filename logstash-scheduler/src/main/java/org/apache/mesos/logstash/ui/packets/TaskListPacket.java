package org.apache.mesos.logstash.ui.packets;

import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.scheduler.Task;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.mesos.logstash.common.LogstashProtos.ContainerState.LoggingStateType.STREAMING;

public class TaskListPacket {

    public List<Task> tasks;

    public static class Task {
        public String slaveId;
        public String executorId;
        public String taskId;
        public long activeStreamCount;
        public List<Container> containers;

        public static Task fromTask(org.apache.mesos.logstash.scheduler.Task task) {
            Task other = new Task();
            other.taskId = task.getTaskId().getValue();
            other.slaveId = task.getSlaveID().getValue();
            other.executorId = task.getExecutorID().getValue();
            other.containers = task.getContainers().stream()
                .map(Container::fromConatainerState).collect(toList());
            other.activeStreamCount = task.getActiveStreamCount();
            return other;
        }
    }

    public static class Container {
        public String name;
        public String status;

        public static Container fromConatainerState(LogstashProtos.ContainerState state) {
            Container container = new Container();
            container.name = state.getName();
            container.status = state.getType().toString();
            return container;
        }
    }

    public static TaskListPacket fromTaskList(Collection<org.apache.mesos.logstash.scheduler.Task> tasks) {
        TaskListPacket packet = new TaskListPacket();
        packet.tasks = tasks.stream().map(Task::fromTask).collect(toList());
        return packet;
    }
}
