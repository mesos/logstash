package org.apache.mesos.logstash.state;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;
/**
 * Tests
 */
@SuppressWarnings({"PMD.TooManyMethods"})
@RunWith(MockitoJUnitRunner.class)
public class SerializableZookeeperStateTest {
    private static final String SERIALIZABLE_OBJECT = "Serializable object";
    @Mock
    State state;
    @Mock
    final Variable variable = mock(Variable.class);

    @InjectMocks
    final
    SerializableState serializableState = new SerializableZookeeperState();

    @Before
    public void before() throws IOException {
        when(variable.value()).thenReturn(writeVariable(SERIALIZABLE_OBJECT));
        Future<Variable> future = CompletableFuture.completedFuture(variable);
        when(state.fetch(anyString())).thenReturn(future);
        when(state.store(any(Variable.class))).thenReturn(future);
    }

    @Test
    public void testSetValid() throws IOException {
        serializableState.set("test", "Serializable object");
        verify(state, times(1)).store(any(Variable.class));
    }

    @Test(expected = IOException.class)
    public void interrupted() throws IOException {
        when(state.store(any(Variable.class))).thenThrow(InterruptedException.class);
        serializableState.set("test", "Serializable object");
    }

    @Test(expected = IOException.class)
    public void executionException() throws IOException {
        when(state.store(any(Variable.class))).thenThrow(ExecutionException.class);
        serializableState.set("test", "Serializable object");
    }

    @Test(expected = IOException.class)
    public void ioException() throws IOException {
        when(state.store(any(Variable.class))).thenThrow(IOException.class);
        serializableState.set("test", "Serializable object");
    }

    @Test
    public void testGetValid() throws IOException {
        String variable = serializableState.get("test");
        verify(state, times(1)).fetch(anyString());
        assertEquals(SERIALIZABLE_OBJECT, variable);
    }

    @Test
    public void testNullIfNodeDoesntExist() throws IOException {
        when(variable.value()).thenReturn(new byte[0]);
        Object variable = serializableState.get("test");
        verify(state, times(1)).fetch(anyString());
        assertNull(variable);
    }

    @Test(expected = IOException.class)
    public void testInterrupted() throws IOException {
        when(state.fetch(anyString())).thenThrow(InterruptedException.class);
        serializableState.get("test");
    }

    @Test(expected = IOException.class)
    public void testClassNotFound() throws IOException {
        when(state.fetch(anyString())).thenThrow(ClassNotFoundException.class);
        serializableState.get("test");
    }

    @Test(expected = IOException.class)
    public void testExecution() throws IOException {
        when(state.fetch(anyString())).thenThrow(ExecutionException.class);
        serializableState.get("test");
    }

    @Test(expected = IOException.class)
    public void testIOException() throws IOException {
        when(state.fetch(anyString())).thenThrow(IOException.class);
        serializableState.get("test");
    }

    @Test(expected = IOException.class)
    public void testInvalidObjectStream() throws IOException {
        when(variable.value()).thenReturn("Invalid stream of bytes".getBytes(Charset.forName("UTF-8")));
        serializableState.get("test");
    }

    @Test
    public void shouldDeleteKey() throws IOException {
        serializableState.delete("test");
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfKeyDoesntExist() throws IOException {
        when(variable.value()).thenReturn("".getBytes(Charset.forName("UTF-8")));
        when(state.fetch(anyString())).thenReturn(CompletableFuture.completedFuture(variable));
        serializableState.delete("test");
    }


    // Data must be serialized/deserialized in exactly the same way to be readable.
    private byte[] writeVariable(Object object) throws IOException {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream out = null;
        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            return bos.toByteArray();
        } finally {
            if (out != null) {
                out.close();
            }
            if (bos != null) {
                bos.close();
            }
        }
    }
}