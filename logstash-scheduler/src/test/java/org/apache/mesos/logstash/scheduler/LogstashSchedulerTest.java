package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
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

        scheduler = new LogstashScheduler(liveState, configuration, configManager, driverFactory);

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


    @Test
    public void shouldNotAcceptOffersWithoutPort5000Available() throws Exception {
        when(clusterMonitor.getClusterState()).thenReturn(clusterState);
        when(clusterState.getTaskList()).thenReturn(emptyList());

        scheduler.clusterMonitor = clusterMonitor;
        scheduler.taskInfoBuilder = taskInfoBuilder;

        scheduler.resourceOffers(driver, asList(createOffer(1.0, 512.0, asList(new PortRange(1, 4999)))));

        verify(taskInfoBuilder, never()).buildTask(any(Protos.Offer.class));
    }

    @Test
    public void shouldAcceptOffersWithPort5000Available() throws Exception {
        when(clusterMonitor.getClusterState()).thenReturn(clusterState);
        when(clusterState.getTaskList()).thenReturn(emptyList());
        when(taskInfoBuilder.buildTask(any(Protos.Offer.class))).thenThrow(new RuntimeException("Offer accepted"));

        scheduler.clusterMonitor = clusterMonitor;
        scheduler.taskInfoBuilder = taskInfoBuilder;

        try {
            scheduler.resourceOffers(driver, asList(createOffer(1.0, 512.0, asList(new PortRange(1, 5000)))));
        } catch (RuntimeException e) {
            // TODO: 23/11/2015 REALLY?!
            assertEquals("Offer accepted", e.getMessage());
        }

        verify(taskInfoBuilder).buildTask(any(Protos.Offer.class));
    }

    private Protos.Offer createOffer(double cpu, double memory, List<PortRange> portRanges) {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("offer-" + System.currentTimeMillis()))
                .setFrameworkId(createFrameworkId("logstash"))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave"))
                .setHostname(RandomStringUtils.randomAlphabetic(8))
                .addResources(Protos.Resource.newBuilder()
                        .setName("ranges")
                        .setType(Protos.Value.Type.RANGES)
                        .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(portRanges.stream().map(PortRange::toProto).collect(Collectors.toList())))
                )
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu))
                )
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(memory))
                )
                .build();
    }

    private static class PortRange {
        final long begin, end;

        private PortRange(long begin, long end) {
            this.begin = begin;
            this.end = end;
        }


        public Protos.Value.Range toProto() {
            return Protos.Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
        }
    }

}
