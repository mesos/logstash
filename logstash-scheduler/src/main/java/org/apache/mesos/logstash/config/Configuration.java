package org.apache.mesos.logstash.config;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.SerializableState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class Configuration {

    private String zookeeperUrl = null;
    private SerializableState state = null;
    private double executorCpus = 0;
    private int executorHeapSize = 0;
    private int logstashHeapSize = 0;
    private String frameworkName = "logstash";
    private long failoverTimeout = 0;
    private String logStashUser = "";
    private int zkTimout =0;
    private String logStashRole = "";
    private FrameworkState frameworkState;
    private boolean disableFailover = false;
    private int reconcilationTimeoutSek = 60 * 1;
    private int executorOverheadMem = 50;
    private int webServerPort = 9092;
    private Optional<String> elasticsearchDomainAndPort = Optional.empty();

    public void setVolumeString(String volumeString) {
        this.volumeString = volumeString;
    }

    private String volumeString = "";

    public int getReconcilationTimeoutMillis() {
        return reconcilationTimeoutSek * 1000;
    }

    public void setReconcilationTimeoutSek(int reconcilationTimeoutSek) {
        this.reconcilationTimeoutSek = reconcilationTimeoutSek;
    }

    public List<String> getVolumes() {
        return splitVolumes(volumeString);
    }

    private static List<String> splitVolumes(String volumeString) {
        if (volumeString.isEmpty()){
            return Collections.emptyList();
        }
        return Arrays.asList(volumeString.split(","));
    }

    // Generate a fingerprint that can be used to compare configurations quickly
    public String getFingerprint() {
        String fingerprint = "EXECUTOR HEAPSIZE " + executorHeapSize + " EXECUTOR CPUS " + executorCpus + "LS HEAP SIZE" + logstashHeapSize;
        fingerprint = fingerprint + "LS USER " + logStashUser + "LOGSTASH ROLE" + logStashRole;
        fingerprint = fingerprint + " VOLUMES " + this.volumeString + "EXECUTOR_VERSION " + LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG;

        return fingerprint;
    }

    public boolean isDisableFailover() {
        return disableFailover;
    }

    public void setDisableFailover(boolean disableFailover) {
        this.disableFailover = disableFailover;
    }



    public String getZookeeperUrl() {
        return zookeeperUrl;
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


    public void setZookeeperUrl(String zookeeperUrl) {
        this.zookeeperUrl = zookeeperUrl;
    }

    public SerializableState getState() {
        return state;
    }

    public void setState(SerializableState state) {
        this.state = state;
    }

    public double getExecutorCpus() {
        return executorCpus;
    }

    public void setExecutorCpus(double executorCpus) {
        this.executorCpus = executorCpus;
    }

    public int getExecutorHeapSize() {
        return executorHeapSize;
    }

    public void setExecutorHeapSize(int executorHeapSize) {
        this.executorHeapSize = executorHeapSize;
    }

    public int getLogstashHeapSize() {
        return logstashHeapSize;
    }

    public void setLogstashHeapSize(int logstashHeapSize) {
        this.logstashHeapSize = logstashHeapSize;
    }
    public Protos.FrameworkID getFrameworkId() {
        return getFrameworkState().getFrameworkID();
    }
    public String getFrameworkName() {
        return frameworkName;
    }

    public void setFrameworkName(String frameworkName) {
        this.frameworkName = frameworkName;
    }

    public long getFailoverTimeout() {
        return failoverTimeout;
    }

    public void setFailoverTimeout(long failoverTimeout) {
        this.failoverTimeout = failoverTimeout;
    }

    public String getLogStashUser() {
        return logStashUser;
    }

    public void setLogStashUser(String logStashUser) {
        this.logStashUser = logStashUser;
    }

    public String getLogStashRole() {
        return logStashRole;
    }

    public void setLogStashRole(String logStashRole) {
        this.logStashRole = logStashRole;
    }

    public int getZkTimout() {
        return zkTimout;
    }

    public void setZkTimout(int zkTimout) {
        this.zkTimout = zkTimout;
    }

    public int getExecutorOverheadMem() {
        return executorOverheadMem;
    }

    public int getWebServerPort() {
        return webServerPort;
    }

    public void setWebServerPort(int webServerPort) {
        this.webServerPort = webServerPort;
    }

    public Optional<String> getElasticsearchDomainAndPort() {
        return elasticsearchDomainAndPort;
    }

    public void setElasticsearchDomainAndPort(Optional<String> elasticsearchDomainAndPort) {
        this.elasticsearchDomainAndPort = elasticsearchDomainAndPort;
    }
}
