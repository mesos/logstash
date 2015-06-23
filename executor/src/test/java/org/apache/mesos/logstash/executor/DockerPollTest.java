package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.command.EventCallback;
import com.github.dockerjava.api.model.Event;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by ero on 22/06/15.
 */
public class DockerPollTest {

    private DockerInfo dockerInfoStub = null;
    private DockerPoll target = null;

    @Before
    public void setUp() {
        dockerInfoStub = mock(DockerInfo.class);
    }

    @Test
    public void testNewContainersDiscoveredOnCreation() {

        //
        // Arrange
        //
        when(dockerInfoStub.getContainersThatWantsLogging()).thenReturn(new HashMap<String, LogstashInfo>() {
            {
                put("TEST_ID_1", new LogstashInfo("LOGGPATH1", "{{}}"));
                put("TEST_ID_2", new LogstashInfo("LOGGPATH2", "{{}}"));
            }
        });
        FrameworkListener frameworkListenerSpy = mock(FrameworkListener.class);
        ArgumentCaptor<Framework> argumentCaptor = ArgumentCaptor.forClass(Framework.class);

        //
        // Act
        //
        target = new DockerPoll(dockerInfoStub);
        target.attach(frameworkListenerSpy);

        //
        // Assert
        //
        verify(frameworkListenerSpy, times(2)).FrameworkAdded(argumentCaptor.capture());

        Framework framework1 = argumentCaptor.getAllValues().get(0);
        assertEquals("TEST_ID_1", framework1.getId());
        assertEquals("LOGGPATH1", framework1.getLogLocation());
        assertEquals("{{}}", framework1.getLogstashFilter());

        Framework framework2 = argumentCaptor.getAllValues().get(1);
        assertEquals("TEST_ID_2", framework2.getId());
        assertEquals("LOGGPATH2", framework2.getLogLocation());
        assertEquals("{{}}", framework2.getLogstashFilter());
    }

    @Test
    public void testNewContainerDiscoveredFromEventStatusStart() {

        //
        // Arrange
        //
        when(dockerInfoStub.getContainersThatWantsLogging())
                .thenReturn(new HashMap<String, LogstashInfo>() {
                    {
                        put("TEST_ID_1", new LogstashInfo("LOGGPATH1", "{{}}"));
                    }
                })
                .thenReturn(new HashMap<String, LogstashInfo>() {
                    {
                        put("TEST_ID_2", new LogstashInfo("LOGGPATH2", "{{}}"));
                    }
                });

        FrameworkListener frameworkListenerSpy = mock(FrameworkListener.class);
        ArgumentCaptor<Framework> argumentCaptor = ArgumentCaptor.forClass(Framework.class);
        ArgumentCaptor<EventCallback> argumentCaptorForCallback = ArgumentCaptor.forClass(EventCallback.class);

        //
        // Act
        //
        target = new DockerPoll(dockerInfoStub);
        target.attach(frameworkListenerSpy);

        //
        // Assert
        //
        verify(dockerInfoStub).attachEventListener(argumentCaptorForCallback.capture());
        argumentCaptorForCallback.getValue().onEvent(new Event("start", "TEST_ID_2", "SLAVE1", 1));

        verify(frameworkListenerSpy, times(2)).FrameworkAdded(argumentCaptor.capture());

        Framework framework1 = argumentCaptor.getAllValues().get(0);
        assertEquals("TEST_ID_1", framework1.getId());
        assertEquals("LOGGPATH1", framework1.getLogLocation());
        assertEquals("{{}}", framework1.getLogstashFilter());

        Framework framework2 = argumentCaptor.getAllValues().get(1);
        assertEquals("TEST_ID_2", framework2.getId());
        assertEquals("LOGGPATH2", framework2.getLogLocation());
        assertEquals("{{}}", framework2.getLogstashFilter());
    }

    @Test
    public void testContainerToBeRemovedFromEventStatusStop() {
        //
        // Arrange
        //
        when(dockerInfoStub.getContainersThatWantsLogging())
                .thenReturn(new HashMap<String, LogstashInfo>() {
                    {
                        put("TEST_ID_1", new LogstashInfo("LOGGPATH1", "{{}}"));
                        put("TEST_ID_2", new LogstashInfo("LOGGPATH2", "{{}}"));
                    }
                })
                .thenReturn(new HashMap<String, LogstashInfo>() {
                    {
                        put("TEST_ID_1", new LogstashInfo("LOGGPATH1", "{{}}"));
                    }
                });

        FrameworkListener frameworkListenerSpy = mock(FrameworkListener.class);
        ArgumentCaptor<Framework> argumentCaptor = ArgumentCaptor.forClass(Framework.class);
        ArgumentCaptor<EventCallback> argumentCaptorForCallback = ArgumentCaptor.forClass(EventCallback.class);

        //
        // Act
        //
        target = new DockerPoll(dockerInfoStub);
        target.attach(frameworkListenerSpy);

        //
        // Assert
        //
        verify(dockerInfoStub).attachEventListener(argumentCaptorForCallback.capture());
        argumentCaptorForCallback.getValue().onEvent(new Event("stop", "TEST_ID_2", "SLAVE1", 1));

        verify(frameworkListenerSpy).FrameworkRemoved(argumentCaptor.capture());

        Framework framework = argumentCaptor.getValue();
        assertEquals("TEST_ID_2", framework.getId());
        assertEquals("LOGGPATH2", framework.getLogLocation());
        assertEquals("{{}}", framework.getLogstashFilter());

    }
}
