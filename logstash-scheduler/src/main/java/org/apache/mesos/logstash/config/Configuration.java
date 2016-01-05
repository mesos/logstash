package org.apache.mesos.logstash.config;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.SerializableState;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Deprecated
@Component
public class Configuration {
    // Generate a fingerprint that can be used to compare configurations quickly
    @Deprecated
    public String getFingerprint() {
        // TODO: 10/12/2015 REMOVE METHOD
        String fingerprint = "EXECUTOR HEAPSIZE " + "TODO" + " EXECUTOR CPUS " + "TODO" + "LS HEAP SIZE" + "TODO";
        fingerprint += "LS USER root LOGSTASH ROLE * EXECUTOR_VERSION " + LogstashConstants.EXECUTOR_IMAGE_NAME_WITH_TAG;

        return fingerprint;
    }
}
