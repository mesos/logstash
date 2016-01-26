package org.apache.mesos.logstash.util;

import org.apache.mesos.Protos;

import java.util.UUID;

/**
 * Proto file helpers for tests.
 */
public class ProtoTestUtil {
    public static Protos.TaskInfo getDefaultTaskInfo() {
        return Protos.TaskInfo.newBuilder()
            .setName("dummyTaskName")
            .setTaskId(Protos.TaskID.newBuilder().setValue(UUID.randomUUID().toString()))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()).build())
            .setExecutor(Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("executorID").build())
                .setCommand(Protos.CommandInfo.newBuilder().setValue("").build())
                .build())

            .build();
    }

    private static Protos.TaskStatus createTaskStatus(Protos.TaskState taskState, String taskId,
                                                      String slaveID, String message) {
        return Protos.TaskStatus.newBuilder()
            .setState(taskState)
            .setMessage(message)
            .setTaskId(Protos.TaskID.newBuilder().setValue(taskId))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveID)).build();
    }
}
