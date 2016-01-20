package org.apache.mesos.logstash.config;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.common.network.NetworkUtils;
import org.apache.mesos.logstash.scheduler.Features;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

@Component
@ConfigurationProperties
public class FrameworkConfig {

    public static final Logger LOGGER = Logger.getLogger(FrameworkConfig.class);

    public static final String LOGSTASH_EXECUTOR_JAR = "logstash-mesos-executor.jar";

    @NotNull
    @Pattern(regexp = "^zk://.+$")
    private String zkUrl;

    private int zkTimeout = 20000;

    private String frameworkName = "logstash";

    private double failoverTimeout = 31449600;

    private long reconcilationTimeoutMillis;

    private String role = "*";
    private String user = "root";

    private String mesosPrincipal = null;
    private String mesosSecret = null;

    private String javaHome = "";

    @Inject
    private Features features;

    @Inject
    private NetworkUtils networkUtils;

    public String getJavaHome() {
        if (!javaHome.isEmpty()) {
            return javaHome.replaceAll("java$", "").replaceAll("/$", "") + "/";
        } else {
            return "";
        }
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

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

    public String getFrameworkFileServerAddress() {
        return networkUtils.addressToString(networkUtils.hostSocket(8080), true);
    }
}
