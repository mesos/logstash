package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "logstash")
public class LogstashConfig {
    private int heapSize = 64;
    private Optional<String> elasticsearchUrl = Optional.empty();
    private String role = "*";
    private String user = "root";

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
}
