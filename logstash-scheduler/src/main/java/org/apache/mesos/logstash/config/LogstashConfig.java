package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "logstash")
public class LogstashConfig {

    private int heapSize = 64;
    private Optional<URL> elasticsearchUrl = Optional.empty();

    private String executorImage = "mesos/logstash-executor";
    private String executorVersion = "latest";
    private int syslogPort = 514;
    private int collectdPort = 25826;

    public int getHeapSize() {
        return heapSize;
    }

    public void setHeapSize(int heapSize) {
        this.heapSize = heapSize;
    }

    public Optional<URL> getElasticsearchUrl() {
        return elasticsearchUrl;
    }

    public void setElasticsearchUrl(Optional<URL> elasticsearchUrl) {
        this.elasticsearchUrl = elasticsearchUrl;
    }

    public String getExecutorImage() {
        return executorImage;
    }

    public void setExecutorImage(String executorImage) {
        this.executorImage = executorImage;
    }

    public String getExecutorVersion() {
        return executorVersion;
    }

    public void setExecutorVersion(String executorVersion) {
        this.executorVersion = executorVersion;
    }

    public int getSyslogPort() {
        return syslogPort;
    }

    public void setSyslogPort(int syslogPort) {
        this.syslogPort = syslogPort;
    }

    public int getCollectdPort() {
        return collectdPort;
    }

    public void setCollectdPort(int collectdPort) {
        this.collectdPort = collectdPort;
    }

    // FIXME how much disk space does Logstash actually require?
    public double getRequiredDiskMegabytes() {
        return 10;
    }
}
