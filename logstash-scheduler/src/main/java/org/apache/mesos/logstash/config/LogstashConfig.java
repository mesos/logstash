package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "logstash")
public class LogstashConfig {

    private int heapSize = 64;
    private String[] elasticsearchHost = new String[0];

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

    public String[] getElasticsearchHost() {
        return elasticsearchHost;
    }

    public void setElasticsearchHost(String[] elasticsearchHost) {
        this.elasticsearchHost = elasticsearchHost;
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
}
