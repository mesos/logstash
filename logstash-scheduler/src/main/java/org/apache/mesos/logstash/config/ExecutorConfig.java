package org.apache.mesos.logstash.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import java.util.Collections;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "executor")
public class ExecutorConfig {
    @DecimalMin(value = "0.0", inclusive = false)
    private double cpus = 0.2;

    @Min(32L)
    private int heapSize = 64;

    private int overheadMem = 50;

    private List<String> filePath = Collections.singletonList("/var/log/messages");

    public double getCpus() {
        return cpus;
    }

    public void setCpus(double cpus) {
        this.cpus = cpus;
    }

    public int getHeapSize() {
        return heapSize;
    }

    public void setHeapSize(int heapSize) {
        this.heapSize = heapSize;
    }

    public int getOverheadMem() {
        return overheadMem;
    }

    public void setOverheadMem(int overheadMem) {
        this.overheadMem = overheadMem;
    }

    public List<String> getFilePath() {
        return filePath;
    }

    public void setFilePath(List<String> filePath) {
        this.filePath = filePath;
    }
}
