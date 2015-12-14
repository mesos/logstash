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


    public int getWebserverPort() {
        return webserverPort;
    }

    public void setWebserverPort(int webserverPort) {
        this.webserverPort = webserverPort;
    }
}
