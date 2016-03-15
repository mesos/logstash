package org.apache.mesos.logstash.config;

import org.apache.mesos.logstash.common.network.NetworkUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.EmbeddedServletContainerInitializedEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

// TODO: Add unit test to assert default settings, URIs and commands

@Component
@ConfigurationProperties
public class FrameworkConfig implements ApplicationListener<EmbeddedServletContainerInitializedEvent> {
    private static final String LOGSTASH_EXECUTOR_JAR   = "logstash-mesos-executor.jar";

    private static final String LOGSTASH_TARBALL = "logstash.tar.gz";

    private String javaHome = "";

    @Inject
    private NetworkUtils networkUtils;

    int httpPort;

    @Override
    public void onApplicationEvent(EmbeddedServletContainerInitializedEvent event) {
        httpPort = event.getEmbeddedServletContainer().getPort();
    }

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

    public String getFrameworkFileServerAddress() {
        return networkUtils.addressToString(networkUtils.hostSocket(httpPort), true);
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
