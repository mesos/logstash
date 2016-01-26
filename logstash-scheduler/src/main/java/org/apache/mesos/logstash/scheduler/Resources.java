package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.Protos;

/**
 * Helper class for building Mesos resources.
 */
class Resources {

    private static final String RESOURCE_PORTS = "ports";
    private static final String RESOURCE_CPUS = "cpus";
    private static final String RESOURCE_MEM = "mem";

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
