package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.ExecutorConfig;
import org.apache.mesos.logstash.config.LogstashConfig;
import org.apache.mesos.logstash.state.ClusterState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.apache.mesos.logstash.scheduler.Resources.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OfferStrategyTest {
    public static final String FRAMEWORK_ROLE = "testRole";
    @Mock
    ExecutorConfig executorConfig;
    @Mock
    LogstashConfig logstashConfig;
    @Mock
    ClusterState clusterState;
    @Mock
    Features features;

    @InjectMocks
    OfferStrategy offerStrategy;

    @Test
    public void willDeclineOfferIfHostIsAlreadyRunningTask() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, validOffer("host1"));
        assertFalse(result.acceptable());
        assertEquals(Arrays.asList("host host1 is already running task TestId"), result.complaints);
    }

    @Test
    public void willDeclineOfferIfOfferDoesNotHaveEnoughCpu() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(executorConfig.getCpus()).thenReturn(1.0);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, baseOfferBuilder("host2").addResources(cpus(0.9, FRAMEWORK_ROLE)).build());
        assertFalse(result.acceptable());
        assertEquals(Arrays.asList("required minimum 1.0 cpus but offer only has 0.9 in total"), result.complaints);
    }

    @Test
    public void willDeclineOfferIfOfferDoesNotHaveEnoughMem() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(executorConfig.getHeapSize()).thenReturn(2048);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, baseOfferBuilder("host2").addResources(cpus(1.0, FRAMEWORK_ROLE)).build());
        assertFalse(result.acceptable());
        assertEquals(Arrays.asList("required minimum 2048.0 mem but offer only has 0.0 in total"), result.complaints);
    }

    @Test
    public void willDeclineOfferIfOfferDoesNotHaveNeededPorts() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(features.isSyslog()).thenReturn(true);
        when(logstashConfig.getSyslogPort()).thenReturn(514);
        when(features.isCollectd()).thenReturn(true);
        when(logstashConfig.getCollectdPort()).thenReturn(25826);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(
                clusterState,
                baseOfferBuilder("host2")
                        .addResources(cpus(1.0, FRAMEWORK_ROLE))
                        .addResources(mem(512, FRAMEWORK_ROLE))
                        .build());
        assertFalse(result.acceptable());
        assertEquals(Arrays.asList("required port 514 but was not in offer", "required port 25826 but was not in offer"), result.complaints);
    }

    @Test
    public void willAcceptValidOffer() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(features.isSyslog()).thenReturn(true);
        when(logstashConfig.getSyslogPort()).thenReturn(514);
        when(features.isCollectd()).thenReturn(true);
        when(logstashConfig.getCollectdPort()).thenReturn(25826);


        final OfferStrategy.OfferResult result = offerStrategy.evaluate(
                clusterState,
                baseOfferBuilder("host2")
                        .addResources(cpus(1.0, FRAMEWORK_ROLE))
                        .addResources(mem(512, FRAMEWORK_ROLE))
                        .addResources(portRange(1, 25826, FRAMEWORK_ROLE))
                        .build());
        assertTrue(result.acceptable());
    }

    @Test
    public void willAcceptValidOfferFromCommonPool() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(features.isSyslog()).thenReturn(true);
        when(logstashConfig.getSyslogPort()).thenReturn(514);
        when(features.isCollectd()).thenReturn(false);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(
                clusterState,
                baseOfferBuilder("host2")
                        .addResources(cpus(1.0, "*"))
                        .addResources(mem(512, "*"))
                        .addResources(portRange(514, 514, "*"))
                        .build());
        assertTrue(result.acceptable());
    }

    @Test
    public void willAcceptValidOfferWhenNoPortsAreNeeded() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(features.isSyslog()).thenReturn(false);
        when(features.isCollectd()).thenReturn(false);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(
                clusterState,
                baseOfferBuilder("host2")
                        .addResources(cpus(1.0, FRAMEWORK_ROLE))
                        .addResources(mem(512, FRAMEWORK_ROLE))
                        .build());
        assertTrue(result.acceptable());
    }

    private Protos.TaskInfo createTask(String hostname) throws InvalidProtocolBufferException {
        return Protos.TaskInfo.newBuilder()
                .setName("Test")
                .setTaskId(Protos.TaskID.newBuilder().setValue("TestId").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(hostname).build())
                .build();
    }

    private Protos.Offer validOffer(String slaveId) {
        return baseOfferBuilder(slaveId)
                .build();
    }

    private Protos.Offer.Builder baseOfferBuilder(String slaveId) {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("offerId").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("testframework").build())
                .setHostname("localhost")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId).build());
    }

}