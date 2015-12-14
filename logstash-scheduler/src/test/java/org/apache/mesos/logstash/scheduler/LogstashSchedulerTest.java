package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.config.*;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.TestSerializableStateImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.inject.Inject;
import java.net.InetAddress;

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

    Configuration configuration = new Configuration();

    LogstashConfig logstashConfig = new LogstashConfig();

    private FrameworkConfig frameworkConfig = new FrameworkConfig();

    @Mock
    ConfigManager configManager;

    FrameworkState frameworkState = new FrameworkState(new TestSerializableStateImpl());

    ArgumentCaptor<Protos.FrameworkInfo> frameworkInfoArgumentCaptor = new ArgumentCaptor<>();

    @InjectMocks
    LogstashScheduler scheduler;

    @Before
    public void setup(){
        configuration.setFrameworkState(frameworkState);

        scheduler.configuration = configuration;
        scheduler.frameworkConfig = frameworkConfig;
        scheduler.logstashConfig = logstashConfig;

        when(driverFactory.createMesosDriver(any(), any(), any())).thenReturn(driver);
    }

    @Test
    public void hasInjected() throws Exception {
        assertNotNull(configManager);
        assertSame(configManager, scheduler.configManager);

    }

    @Test
    public void onStartShouldCreateAndStartFramework() throws Exception {
        scheduler.start();

        verify(driverFactory).createMesosDriver(eq(scheduler), frameworkInfoArgumentCaptor.capture(), eq(frameworkConfig.getZkUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getName(), frameworkConfig.getFrameworkName());
        assertEquals("root", frameworkInfo.getUser());
        assertEquals("*", frameworkInfo.getRole());
        assertEquals(frameworkInfo.hasCheckpoint(), true);
        assertEquals((int)frameworkInfo.getFailoverTimeout(),(int) frameworkConfig.getFailoverTimeout());
        assertEquals(frameworkInfo.getWebuiUrl(),"http:\\/\\/" + InetAddress.getLocalHost().getHostName() + ":" + frameworkConfig.getWebserverPort());
        assertEquals(frameworkInfo.getId().getValue(), configuration.getFrameworkId().getValue());

        verify(driver).start();
    }

    @Test
    public void onStartShouldCreateFramework_withNoPersistedFrameworkID() throws Exception {
        scheduler.start();

        verify(driverFactory).createMesosDriver(eq(scheduler), frameworkInfoArgumentCaptor.capture(), eq(frameworkConfig.getZkUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getId().getValue(), "");
    }

    @Test
    public void onStartShouldCreateFramework_withPersistedFrameworkID() throws Exception {
        frameworkState.setFrameworkId(createFrameworkId("FOO"));
        scheduler.start();

        verify(driverFactory).createMesosDriver(eq(scheduler), frameworkInfoArgumentCaptor.capture(), eq(frameworkConfig.getZkUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getId().getValue(), "FOO");
    }

    @Test
    public void onStopShouldDeRegisterConfigManagerOnConfigUpdate() throws Exception {
        scheduler.start();
        scheduler.stop();

        verify(configManager).setOnConfigUpdate(null);
    }

    @Test
    public void onStopShouldWithFailoverIfConfiguredAsFailoverEnabled() throws Exception {
        configuration.setDisableFailover(false);
        scheduler.start();
        scheduler.stop();

        verify(driver).stop(true);
    }

    @Test
    public void onStopWithFailoverIfConfiguredAsFailoverDisabled_shouldStop() throws Exception {
        frameworkState.setFrameworkId(createFrameworkId("FOO"));
        configuration.setDisableFailover(true);
        scheduler.start();
        scheduler.stop();

        verify(driver).stop(false);
    }

    @Test
    public void onStopWithFailoverIfConfiguredAsFailoverDisabled_shouldRemovePersistedFrameworkID() throws Exception {
        frameworkState.setFrameworkId(createFrameworkId("FOO"));
        configuration.setDisableFailover(true);
        scheduler.start();
        scheduler.stop();

        assertEquals(frameworkState.getFrameworkID().getValue(), "");
    }


    private Protos.FrameworkID createFrameworkId(String frameworkId) {
        return Protos.FrameworkID.newBuilder().setValue(frameworkId).build();
    }
}
