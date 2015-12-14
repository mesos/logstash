package org.apache.mesos.logstash.config;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.SerializableState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Deprecated
public class Configuration {

    private SerializableState state = null;
    private long failoverTimeout = 0;
    private String logStashUser = "";
    private String logStashRole = "";
    private FrameworkState frameworkState;
    private boolean disableFailover = false;
    private int reconcilationTimeoutSek = 60 * 1;

    public int getReconcilationTimeoutMillis() {
        return reconcilationTimeoutSek * 1000;
    }

    public void setReconcilationTimeoutSek(int reconcilationTimeoutSek) {
        this.reconcilationTimeoutSek = reconcilationTimeoutSek;
    }

    private static List<String> splitVolumes(String volumeString) {
        if (volumeString.isEmpty()){
            return Collections.emptyList();
        }
        return Arrays.asList(volumeString.split(","));
    }

    // Generate a fingerprint that can be used to compare configurations quickly
    public String getFingerprint() {
        // TODO: 10/12/2015 REMOVE METHOD
        String fingerprint = "EXECUTOR HEAPSIZE " + "TODO" + " EXECUTOR CPUS " + "TODO" + "LS HEAP SIZE" + "TODO";
        fingerprint += "LS USER " + logStashUser + "LOGSTASH ROLE" + logStashRole;
        fingerprint += "EXECUTOR_VERSION " + LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG;

        return fingerprint;
    }

    public boolean isDisableFailover() {
        return disableFailover;
    }

    public void setDisableFailover(boolean disableFailover) {
        this.disableFailover = disableFailover;
    }



    public void setFrameworkState(FrameworkState frameworkState) {
        this.frameworkState = frameworkState;
    }

    public FrameworkState getFrameworkState() {
        if (frameworkState == null) {
            frameworkState = new FrameworkState(state);
        }
        return frameworkState;
    }


    public SerializableState getState() {
        return state;
    }

    public void setState(SerializableState state) {
        this.state = state;
    }

    public Protos.FrameworkID getFrameworkId() {
        return getFrameworkState().getFrameworkID();
    }
}
