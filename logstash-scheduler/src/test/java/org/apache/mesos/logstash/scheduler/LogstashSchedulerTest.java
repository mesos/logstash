package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.config.*;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.SerializableState;
import org.apache.mesos.logstash.state.TestSerializableStateImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests Scheduler API.
 */
@RunWith(MockitoJUnitRunner.class)
public class LogstashSchedulerTest {
    @Mock
    MesosSchedulerDriverFactory driverFactory;

    @Mock
    SchedulerDriver driver;

    Features features = new Features();

    private FrameworkConfig frameworkConfig = new FrameworkConfig();

    @Mock
    ConfigManager configManager;

    @Mock
    SerializableState serializableState;

    @Mock
    FrameworkState frameworkState;

    ArgumentCaptor<Protos.FrameworkInfo> frameworkInfoArgumentCaptor = new ArgumentCaptor<>();

    @InjectMocks
    LogstashScheduler scheduler;

    @Before
    public void setup() throws Exception {
        scheduler.frameworkConfig = frameworkConfig;
        scheduler.features = features;

        when(driverFactory.createMesosDriver(any(), any(), any())).thenReturn(driver);
    }

    @Test
    public void hasInjected() throws Exception {
        assertNotNull(configManager);
        assertSame(configManager, scheduler.configManager);
    }

    @Test
    public void onStartShouldCreateAndStartFramework() throws Exception {
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));

        scheduler.start();

        verify(driverFactory).createMesosDriver(eq(scheduler), frameworkInfoArgumentCaptor.capture(), eq(frameworkConfig.getZkUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getName(), frameworkConfig.getFrameworkName());
        assertEquals("root", frameworkInfo.getUser());
        assertEquals("*", frameworkInfo.getRole());
        assertEquals(frameworkInfo.hasCheckpoint(), true);
        assertEquals((int)frameworkInfo.getFailoverTimeout(),(int) frameworkConfig.getFailoverTimeout());
        assertEquals(frameworkInfo.getId().getValue(), frameworkState.getFrameworkID().getValue());

        verify(driver).start();
    }

    @Test
    public void onStartShouldCreateFramework_withNoPersistedFrameworkID() throws Exception {
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId(""));
        scheduler.start();

        verify(driverFactory).createMesosDriver(eq(scheduler), frameworkInfoArgumentCaptor.capture(), eq(frameworkConfig.getZkUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals("", frameworkInfo.getId().getValue());
    }

    @Test
    public void onStartShouldCreateFramework_withPersistedFrameworkID() throws Exception {
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));
        scheduler.start();

        verify(driverFactory).createMesosDriver(eq(scheduler), frameworkInfoArgumentCaptor.capture(), eq(frameworkConfig.getZkUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals("test", frameworkInfo.getId().getValue());
    }

    @Test
    public void onStopShouldDeRegisterConfigManagerOnConfigUpdate() throws Exception {
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));
        scheduler.start();
        scheduler.stop();

        verify(configManager).setOnConfigUpdate(null);
    }

    @Test
    public void onStopShouldWithFailoverIfConfiguredAsFailoverEnabled() throws Exception {
        features.setFailover(true);
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));
        scheduler.start();
        scheduler.stop();

        verify(driver).stop(true);
    }

    @Test
    public void onStopWithFailoverIfConfiguredAsFailoverDisabled_shouldStop() throws Exception {
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));
        features.setFailover(false);
        scheduler.start();
        scheduler.stop();

        verify(driver).stop(false);
    }

    @Test
    public void onStopWithFailoverIfConfiguredAsFailoverDisabled_shouldRemovePersistedFrameworkID() throws Exception {
        features.setFailover(false);
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId(""));
        scheduler.start();
        scheduler.stop();

        assertEquals("", frameworkState.getFrameworkID().getValue());
    }


    private Protos.FrameworkID createFrameworkId(String frameworkId) {
        return Protos.FrameworkID.newBuilder().setValue(frameworkId).build();
    }
}
