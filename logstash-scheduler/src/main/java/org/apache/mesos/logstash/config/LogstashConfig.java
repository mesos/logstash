package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "logstash")
public class LogstashConfig {
    private int heapSize = 32;
    private Optional<String> elasticsearchUrl = Optional.empty();

    public int getHeapSize() {
        return heapSize;
    }

    public void setHeapSize(int heapSize) {
        this.heapSize = heapSize;
    }

    public Optional<String> getElasticsearchUrl() {
        return elasticsearchUrl;
    }

    public void setElasticsearchUrl(Optional<String> elasticsearchUrl) {
        this.elasticsearchUrl = elasticsearchUrl;
    }
}
