package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.cluster.ClusterMonitor;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.ExecutorConfig;
import org.apache.mesos.logstash.config.FrameworkConfig;
import org.apache.mesos.logstash.config.LogstashConfig;
import org.apache.mesos.logstash.state.*;
import org.apache.mesos.logstash.util.Clock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class LogstashSchedulerSpringTest {
    static Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue("test").build();

    @Autowired
    LogstashScheduler logstashScheduler;

    @Autowired
    FrameworkState frameworkState;

    @Configuration
    static class Config {

        @Bean
        public LogstashScheduler logstashScheduler() {
            return new LogstashScheduler();
        }

        @Bean
        public ConfigManager configManager() {
            return mock(ConfigManager.class);
        }

        @Bean
        public LiveState liveState() {
            return mock(LiveState.class);
        }

        @Bean
        public Features features() {
            return Mockito.mock(Features.class);
        }

        @Bean
        public FrameworkConfig frameworkConfig() {
            FrameworkConfig mock = Mockito.mock(FrameworkConfig.class);
            when(mock.getFrameworkName()).thenReturn("Logstash");
            when(mock.getFailoverTimeout()).thenReturn(31449600.0);
            return mock;
        }

        @Bean
        public LogstashConfig logstashConfig() {
            LogstashConfig mock = Mockito.mock(LogstashConfig.class);
            when(mock.getUser()).thenReturn("root");
            when(mock.getRole()).thenReturn("*");
            return mock;
        }

        @Bean
        public TaskInfoBuilder taskInfoBuilder() {
            return Mockito.mock(TaskInfoBuilder.class);
        }

        @Bean
        public MesosSchedulerDriverFactory mesosSchedulerDriverFactory() {
            MesosSchedulerDriverFactory mock = Mockito.mock(MesosSchedulerDriverFactory.class);
            when(mock.createMesosDriver(any(LogstashScheduler.class), any(Protos.FrameworkInfo.class), anyString())).thenReturn(mock(SchedulerDriver.class));
            return mock;
        }

        @Bean
        public OfferStrategy offerStrategy() {
            return Mockito.mock(OfferStrategy.class);
        }

        @Bean
        public Clock clock() {
            return Mockito.mock(Clock.class);
        }

        @Bean
        public ExecutorConfig executorConfig() {
            return Mockito.mock(ExecutorConfig.class);
        }

        @Bean
        public SerializableState serializableState() {
            return Mockito.mock(SerializableState.class);
        }

        @Bean
        public FrameworkState frameworkState() {
            return Mockito.mock(FrameworkState.class);
        }

        @Bean
        public StatePath statePath() {
            return Mockito.mock(StatePath.class);
        }

        @Bean
        public ClusterMonitor clusterMonitor() {
            return Mockito.mock(ClusterMonitor.class);
        }

        @Bean
        public ClusterState clusterState() {
            return Mockito.mock(ClusterState.class);
        }
    }

    @Test
    public void isInjecting() throws Exception {
        assertNotNull(logstashScheduler);
        logstashScheduler.registered(mock(SchedulerDriver.class), frameworkId, Protos.MasterInfo.newBuilder().setId("test").setIp(1).setPort(1).build());
        verify(frameworkState).setFrameworkId(frameworkId);
    }
}
