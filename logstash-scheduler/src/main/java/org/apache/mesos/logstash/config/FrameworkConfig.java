package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

@Component
@ConfigurationProperties
public class FrameworkConfig {
    @NotNull
    @Pattern(regexp = "^zk://.+$")
    private String zkUrl;

    private int zkTimeout = 20000;

    private String frameworkName = "logstash";
    private int webserverPort = 9092;
    private double failoverTimeout = 31449600;
    private long reconcilationTimeoutMillis;

    private String role = "*";
    private String user = "root";

    private Integer syslogPort;

    private Integer collectdPort;

    public String getZkUrl() {
        return zkUrl;
    }

    public void setZkUrl(String zkUrl) {
        this.zkUrl = zkUrl;
    }

    public int getZkTimeout() {
        return zkTimeout;
    }

    public void setZkTimeout(int zkTimeout) {
        this.zkTimeout = zkTimeout;
    }

    public String getFrameworkName() {
        return frameworkName;
    }

    public void setFrameworkName(String frameworkName) {
        this.frameworkName = frameworkName;
    }

    public double getFailoverTimeout() {
        return failoverTimeout;
    }

    public void setFailoverTimeout(double failoverTimeout) {
        this.failoverTimeout = failoverTimeout;
    }

    public long getReconcilationTimeoutMillis() {
        return reconcilationTimeoutMillis;
    }

    public void setReconcilationTimeoutMillis(long reconcilationTimeoutMillis) {
        this.reconcilationTimeoutMillis = reconcilationTimeoutMillis;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Integer getCollectdPort() {
        return collectdPort;
    }

    public void setCollectdPort(Integer collectdPort) {
        this.collectdPort = collectdPort;
    }

    public Integer getSyslogPort() {
        return syslogPort;
    }

    public void setSyslogPort(Integer syslogPort) {
        this.syslogPort = syslogPort;
    }

    public int getNumberOfPorts() {
        int numberOfPorts = 0;
        numberOfPorts += (collectdPort != null ? 1 : 0);
        numberOfPorts += (syslogPort != null ? 1 : 0);
        return numberOfPorts;
    }
}
