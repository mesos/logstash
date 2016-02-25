package org.apache.mesos.logstash.config;

import org.apache.mesos.logstash.common.network.NetworkUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

// TODO: Add unit test to assert default settings, URIs and commands

@Component
@ConfigurationProperties
public class FrameworkConfig {

    private static final String LOGSTASH_EXECUTOR_JAR   = "logstash-mesos-executor.jar";

    private static final String LOGSTASH_TARBALL = "logstash.tar.gz";

    private String frameworkName = "logstash";

    private double failoverTimeout = 31449600;

    private long reconcilationTimeoutMillis;

    private String mesosRole = "*";
    private String mesosUser = "root";

    private String mesosPrincipal = null;
    private String mesosSecret = null;

    private String javaHome = "";

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

    public String getFrameworkFileServerAddress() {
        return networkUtils.addressToString(networkUtils.hostSocket(8080), true);
    }

    public String getLogstashTarballUri() {
        return getFrameworkFileServerAddress() + "/" + FrameworkConfig.LOGSTASH_TARBALL;
    }

    public String getLogstashExecutorUri() {
        return getFrameworkFileServerAddress() + "/" + FrameworkConfig.LOGSTASH_EXECUTOR_JAR;
    }

    public String getExecutorCommand() {
        return getJavaHome() + "java $JAVA_OPTS -jar ./" + FrameworkConfig.LOGSTASH_EXECUTOR_JAR;
    }

}
