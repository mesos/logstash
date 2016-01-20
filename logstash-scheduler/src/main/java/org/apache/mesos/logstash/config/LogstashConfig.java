package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "logstash")
public class LogstashConfig {

    private static final String DEFAULT_EXECUTOR_IMAGE_NAME = "mesos/logstash-executor";
    private static final String DEFAULT_EXECUTOR_IMAGE_TAG = "latest";

    private int heapSize = 64;
    private Optional<URL> elasticsearchUrl = Optional.empty();

    private String executorImage = DEFAULT_EXECUTOR_IMAGE_NAME;
    private String executorVersion = DEFAULT_EXECUTOR_IMAGE_TAG;

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
}
