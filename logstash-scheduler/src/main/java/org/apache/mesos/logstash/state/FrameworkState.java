package org.apache.mesos.logstash.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;

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

    public void removeFrameworkId() {
        try {
            state.delete(FRAMEWORKID_KEY);
        } catch (IOException e) {
            LOGGER.error("Unable to remove framework ID in zookeeper", e);
        }
    }
}