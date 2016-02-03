package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.util.ProtoTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.InvalidParameterException;

import static org.mockito.Mockito.*;

/**
 * Tests
 */
public class LSTaskStatusTest {
    private final SerializableState state = mock(SerializableState.class);
    private Protos.FrameworkID frameworkID;
    private Protos.TaskInfo taskInfo;
    private LSTaskStatus status;
    private Protos.TaskStatus taskStatus;

    @Before
    public void before() {
        frameworkID = Protos.FrameworkID.newBuilder().setValue("FrameworkId").build();
        taskInfo = ProtoTestUtil.getDefaultTaskInfo();
        status = new LSTaskStatus(state, frameworkID, taskInfo);
        taskStatus = status.getDefaultStatus();
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleSetException() throws IllegalStateException, IOException {
        doThrow(IOException.class).when(state).set(anyString(), any());
        status.setStatus(taskStatus);
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleGetException() throws IllegalStateException, IOException {
        doThrow(IOException.class).when(state).get(anyString());
        status.getStatus();
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfStateIsNull() {
        new LSTaskStatus(null, frameworkID, taskInfo);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfFrameworkIDIsNull() {
        new LSTaskStatus(state, null, taskInfo);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfFrameworkIDIsEmpty() {
        new LSTaskStatus(state, Protos.FrameworkID.newBuilder().setValue("").build(), taskInfo);
    }

    @Test
    public void shouldAllowNullTaskInfo() {
        new LSTaskStatus(state, frameworkID, null);
    }

    @Test
    public void shouldPrintOk() throws IOException {
        when(state.get(anyString())).thenReturn(status.getDefaultStatus());
        String s = status.toString();
        Assert.assertTrue(s.contains(Protos.TaskState.TASK_STARTING.toString()));
        Assert.assertTrue(s.contains(LSTaskStatus.DEFAULT_STATUS_NO_MESSAGE_SET));
    }

    @Test
    public void shouldHandleNoMessageError() {
        status.toString();
    }
}