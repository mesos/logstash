package org.apache.mesos.logstash.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage.SchedulerMessageType.NEW_CONFIG;

/**
 * Model of framework state
 */
@Component
public class FrameworkState {
    private static final Logger LOGGER = Logger.getLogger(FrameworkState.class);
    private static final String FRAMEWORKID_KEY = "frameworkId";
    public static final Protos.FrameworkID EMPTY_ID = Protos.FrameworkID.newBuilder().setValue("").build();
    private static final String LATEST_CONFIG_KEY = "latestConfig";

    @Inject
    SerializableState state;

    /**
     * Return empty if no frameworkId found.
     */
    public Protos.FrameworkID getFrameworkID() {
        Protos.FrameworkID id = null;
        try {
            id = state.get(FRAMEWORKID_KEY);
        } catch (IOException e) {
            LOGGER.warn("Unable to get FrameworkID from zookeeper", e);
        }
        return id == null ? EMPTY_ID : id;
    }

    public void setFrameworkId(Protos.FrameworkID frameworkId) {
        try {
            SerializableZookeeperState.mkdirAndSet(FRAMEWORKID_KEY, frameworkId, state);
        } catch (IOException e) {
            LOGGER.error("Unable to store framework ID in zookeeper", e);
        }
    }

    public void removeFrameworkId(){
        try {
            state.delete(FRAMEWORKID_KEY);
        } catch (IOException e) {
            LOGGER.error("Unable to remove framework ID in zookeeper", e);
        }
    }

    public void setLatestConfig(List<LogstashProtos.LogstashConfig> configs) {
        LogstashProtos.SchedulerMessage message = LogstashProtos.SchedulerMessage.newBuilder()
            .addAllConfigs(configs)
            .setType(NEW_CONFIG)
            .build();
        try {
            SerializableZookeeperState.mkdirAndSet(LATEST_CONFIG_KEY, message, state);
        } catch (IOException e){
            LOGGER.error("Unable to store logstash configurations in zookeeper", e);
        }
    }


    public LogstashProtos.SchedulerMessage getLatestConfig() {

        try {
           return state.get(LATEST_CONFIG_KEY);

        } catch (IOException e) {
            LOGGER.warn("Unable to get latest logstash configuration from zookeeper", e);
        }
        return null;
    }

}