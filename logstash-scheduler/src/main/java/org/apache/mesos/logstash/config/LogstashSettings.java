package org.apache.mesos.logstash.config;


public class LogstashSettings {

    private final static double DEFAULT_CPUS = 0.2;
    private final static double DEFAULT_MEM = 256;

    private final Double cpus;
    private final Double mem;

    public LogstashSettings(Double cpus, Double mem) {

        this.cpus = cpus;
        this.mem = mem;
    }

    public double getMemForTask() {
        return mem != null ? mem : DEFAULT_MEM;
    }

    public double getCpuForTask() {
        return cpus != null ? cpus : DEFAULT_CPUS;
    }
}
