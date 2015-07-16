package org.apache.mesos.logstash.scheduler.ui;

public class StatMessage {
    private String type = "STATS";

    private int numNodes;
    private double cpus;
    private double mem;
    private double disk;

    public StatMessage(int numNodes, double cpus, double mem, double disk) {
        this.numNodes = numNodes;
        this.cpus = cpus;
        this.mem = mem;
        this.disk = disk;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

    public double getDisk() {
        return disk;
    }

    public String getType() {
        return type;
    }
}