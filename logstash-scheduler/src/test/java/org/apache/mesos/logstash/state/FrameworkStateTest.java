package org.apache.mesos.logstash.state;

import org.apache.mesos.Protos;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests
 */
@RunWith(MockitoJUnitRunner.class)
public class FrameworkStateTest {

    public static final Protos.FrameworkID FRAMEWORK_ID = Protos.FrameworkID.newBuilder().setValue("FrameworkID").build();
    @Mock
    SerializableState state;

    @InjectMocks
    FrameworkState frameworkState = new FrameworkState();

    @Test
    public void testSetFrameworkID() throws IOException {
        frameworkState.setFrameworkId(FRAMEWORK_ID);
        verify(state, times(1)).set(anyString(), eq(FRAMEWORK_ID));
    }

    @Test
    public void testGetFrameworkID() throws IOException {
        when(state.get(anyString())).thenReturn(FRAMEWORK_ID);
        Protos.FrameworkID frameworkID = frameworkState.getFrameworkID();
        verify(state, times(1)).get(anyString());
        assertEquals(FRAMEWORK_ID, frameworkID);
    }

    @Test
    public void testGetEmptyWhenNoFrameworkID() throws IOException {
        Protos.FrameworkID frameworkID = frameworkState.getFrameworkID();
        verify(state, times(1)).get(anyString());
        assertEquals("", frameworkID.getValue());
    }

//    @Test
//    public void testHandleSetException() throws IOException {
//        doThrow(IOException.class).when(state).set(anyString(), any());
//        frameworkState.setFrameworkId(FRAMEWORK_ID);
//    }

    @Test
    public void testHandleGetException() throws IOException {
        doThrow(IOException.class).when(state).get(anyString());
        frameworkState.getFrameworkID();
    }
}