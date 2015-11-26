package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.cluster.ClusterMonitor;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.TestSerializableStateImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests Scheduler API.
 */
public class LogstashSchedulerTest {

    LogstashScheduler scheduler;


    MesosSchedulerDriverFactory driverFactory;


    SchedulerDriver driver;

    private Configuration configuration;
    private ConfigManager configManager;
    private FrameworkState frameworkState ;

    final ClusterMonitor clusterMonitor = mock(ClusterMonitor.class);
    final ClusterState clusterState = mock(ClusterState.class);
    final TaskInfoBuilder taskInfoBuilder = mock(TaskInfoBuilder.class);


    ArgumentCaptor<Protos.FrameworkInfo> frameworkInfoArgumentCaptor = new ArgumentCaptor<>();

    @Before
    public void setup(){
        LiveState liveState = new LiveState();

        configuration = new Configuration();
        frameworkState = new FrameworkState(new TestSerializableStateImpl());
        configuration.setFrameworkState(frameworkState);

        // mocks
        configManager = mock(ConfigManager.class);
        driverFactory = mock(MesosSchedulerDriverFactory.class);
        driver = mock(SchedulerDriver.class);

        scheduler = new LogstashScheduler(liveState, configuration, configManager, driverFactory, mock(OfferStrategy.class));

        when(driverFactory.createMesosDriver(any(), any(), any())).thenReturn(driver);
    }

    @Test
    public void onStartShouldCreateAndStartFramework() throws Exception {
        scheduler.start();

        verify(driverFactory, times(1)).createMesosDriver(eq(scheduler),
            frameworkInfoArgumentCaptor.capture(), eq(configuration.getZookeeperUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getName(), configuration.getFrameworkName());
        assertEquals(frameworkInfo.getUser(), configuration.getLogStashUser());
        assertEquals(frameworkInfo.getRole(), configuration.getLogStashRole());
        assertEquals(frameworkInfo.hasCheckpoint(), true);
        assertEquals((int)frameworkInfo.getFailoverTimeout(),(int) configuration.getFailoverTimeout());
        assertEquals(frameworkInfo.getWebuiUrl(),"http:\\/\\/" + InetAddress.getLocalHost().getHostName() + ":" + configuration.getWebServerPort());
        assertEquals(frameworkInfo.getId().getValue(), configuration.getFrameworkId().getValue());

        verify(driver, times(1)).start();
    }

    @Test
    public void onStartShouldCreateFramework_withNoPersistedFrameworkID() throws Exception {
        scheduler.start();

        verify(driverFactory, times(1)).createMesosDriver(eq(scheduler),
            frameworkInfoArgumentCaptor.capture(), eq(configuration.getZookeeperUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getId().getValue(), "");
    }

    @Test
    public void onStartShouldCreateFramework_withPersistedFrameworkID() throws Exception {
        frameworkState.setFrameworkId(createFrameworkId("FOO"));
        scheduler.start();

        verify(driverFactory, times(1)).createMesosDriver(eq(scheduler),
            frameworkInfoArgumentCaptor.capture(), eq(configuration.getZookeeperUrl()));

        Protos.FrameworkInfo frameworkInfo = frameworkInfoArgumentCaptor.getValue();
        assertEquals(frameworkInfo.getId().getValue(), "FOO");
    }

    @Test
    public void onStopShouldDeRegisterConfigManagerOnConfigUpdate() throws Exception {
        scheduler.start();
        scheduler.stop();

        verify(configManager,times(1)).setOnConfigUpdate(null);
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
