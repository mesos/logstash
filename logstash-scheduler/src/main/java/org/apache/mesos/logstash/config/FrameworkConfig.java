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
    private double failoverTimeout = 31449600;
    private long reconcilationTimeoutMillis;

    private String mesosRole = "*";
    private String mesosUser = "root";

    private String mesosPrincipal = null;
    private String mesosSecret = null;

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

    public String getMesosRole() {
        return mesosRole;
    }

    public void setMesosRole(String mesosRole) {
        this.mesosRole = mesosRole;
    }

    public String getMesosUser() {
        return mesosUser;
    }

    public void setMesosUser(String mesosUser) {
        this.mesosUser = mesosUser;
    }

    public void setMesosPrincipal(String mesosPrincipal) {
        this.mesosPrincipal = mesosPrincipal;
    }

    public String getMesosPrincipal() {
        return mesosPrincipal;
    }

    public void setMesosSecret(String mesosSecret) {
        this.mesosSecret = mesosSecret;
    }

    public String getMesosSecret() {
        return mesosSecret;
    }
}
