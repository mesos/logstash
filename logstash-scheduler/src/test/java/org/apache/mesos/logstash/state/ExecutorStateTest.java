package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Test that Executor state can be set/get
 */
public class ExecutorStateTest {

    private static final String FRAMEWORK_ID = "frameworkId";
    private static final String EXECUTOR_ID = "executorId";
    private static final String TASK_ID = "task1";
    private static final String SLAVE_ID = "slaveID";

    @Test
    public void testExecutorStateMechanism() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException {
        Protos.TaskStatus taskStatus = Protos.TaskStatus.getDefaultInstance();
        SerializableState state = Mockito.mock(SerializableState.class);
        when(state.get(anyString())).thenReturn(taskStatus);
        Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue(SLAVE_ID).build();
        Protos.ExecutorID executorID = Protos.ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();
        Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASK_ID).build();
        Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                .setTaskId(taskID)
                .setExecutor(Protos.ExecutorInfo.newBuilder()
                                .setExecutorId(executorID)
                                .setCommand(Protos.CommandInfo.getDefaultInstance())
                )
                .setSlaveId(slaveID)
                .setName("Test")
                .build();
        LSTaskStatus executorState = new LSTaskStatus(state, frameworkID, taskInfo);

        executorState.setStatus(taskStatus);
        verify(state, times(1)).set(anyString(), any(Protos.TaskStatus.class));

        Protos.TaskStatus newStatus = executorState.getStatus();
        assertNotNull(newStatus);
        assertEquals(taskStatus.getExecutorId().toString(), newStatus.getExecutorId().toString());
    }
}