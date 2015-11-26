package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;

/**
 * Helper class for building Mesos resources.
 */
public class Resources {

    public static final String RESOURCE_PORTS = "ports";
    public static final String RESOURCE_CPUS = "cpus";
    public static final String RESOURCE_MEM = "mem";

    private Resources() {

    }

    public static Protos.Resource portRange(long beginPort, long endPort, String frameworkRole) {
        Protos.Value.Range singlePortRange = Protos.Value.Range.newBuilder().setBegin(beginPort).setEnd(endPort).build();
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_PORTS)
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addRange(singlePortRange))
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource singlePortRange(long port, String frameworkRole) {
        return portRange(port, port, frameworkRole);
    }

    public static Protos.Resource cpus(double cpus, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource mem(double mem, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_MEM)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .setRole(frameworkRole)
                .build();
    }
}
