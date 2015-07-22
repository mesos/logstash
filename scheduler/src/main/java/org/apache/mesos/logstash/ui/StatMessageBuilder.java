package org.apache.mesos.logstash.ui;

public class StatMessageBuilder {
    private int numNodes;
    private double cpus;
    private double mem;
    private double disk;

    public StatMessageBuilder setNumNodes(int numNodes) {
        this.numNodes = numNodes;
        return this;
    }

    public StatMessageBuilder setCpus(double cpus) {
        this.cpus = cpus;
        return this;
    }

    public StatMessageBuilder setMem(double mem) {
        this.mem = mem;
        return this;
    }

    public StatMessageBuilder setDisk(double disk) {
        this.disk = disk;
        return this;
    }

    public StatMessage build() {
        return new StatMessage(numNodes, cpus, mem, disk);
    }
}