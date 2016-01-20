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

import static java.util.Collections.singletonList;
import static org.apache.mesos.logstash.scheduler.Resources.cpus;
import static org.apache.mesos.logstash.scheduler.Resources.mem;
import static org.apache.mesos.logstash.scheduler.Resources.portRange;
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

    @InjectMocks
    OfferStrategy offerStrategy;

    @Test
    public void willDeclineOfferIfHostIsAlreadyRunningTask() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, validOffer("host1"));
        assertFalse(result.acceptable);
        assertEquals("Host already running task", result.reason.get());
    }

    @Test
    public void willDeclineOfferIfOfferDoesNotHaveEnoughCpu() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(executorConfig.getCpus()).thenReturn(1.0);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, baseOfferBuilder("host2").addResources(cpus(0.9, FRAMEWORK_ROLE)).build());
        assertFalse(result.acceptable);
        assertEquals("Offer did not have enough CPU resources", result.reason.get());
    }

    @Test
    public void willDeclineOfferIfOfferDoesNotHaveEnoughMem() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));
        when(executorConfig.getHeapSize()).thenReturn(2048);

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(clusterState, baseOfferBuilder("host2").addResources(cpus(1.0, FRAMEWORK_ROLE)).build());
        assertFalse(result.acceptable);
        assertEquals("Offer did not have enough RAM resources", result.reason.get());
    }

    @Test
    public void willDeclineOfferIfOfferDoesNotHaveNeededPorts() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(
                clusterState,
                baseOfferBuilder("host2")
                        .addResources(cpus(1.0, FRAMEWORK_ROLE))
                        .addResources(mem(512, FRAMEWORK_ROLE))
                        .build());
        assertFalse(result.acceptable);
        assertEquals("Offer did not have ports available", result.reason.get());
    }

    @Test
    public void willAcceptValidOffer() throws Exception {
        when(clusterState.getTaskList()).thenReturn(singletonList(createTask("host1")));

        final OfferStrategy.OfferResult result = offerStrategy.evaluate(
                clusterState,
                baseOfferBuilder("host2")
                        .addResources(cpus(1.0, FRAMEWORK_ROLE))
                        .addResources(mem(512, FRAMEWORK_ROLE))
                        .addResources(portRange(1, 25826, FRAMEWORK_ROLE))
                        .build());
        assertTrue(result.acceptable);
        assertFalse(result.reason.isPresent());
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