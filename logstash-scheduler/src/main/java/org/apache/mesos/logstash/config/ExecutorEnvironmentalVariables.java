package org.apache.mesos.logstash.config;

import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;

/**
 * Environmental variables for the executor
 */
public class ExecutorEnvironmentalVariables {
    private static final String native_mesos_library_key = "MESOS_NATIVE_JAVA_LIBRARY";
    private static final String native_mesos_library_path = "/usr/lib/libmesos.so"; // libmesos.so is usually symlinked to the version.
    private static final String JAVA_OPTS = "JAVA_OPTS";
    private final List<Protos.Environment.Variable> envList = new ArrayList<>();

    /**
     * @param logstashConfig
     */
    public ExecutorEnvironmentalVariables(ExecutorConfig executorConfig, LogstashConfig logstashConfig) {
        populateEnvMap(executorConfig, logstashConfig);
    }

    /**
     * Get a list of environmental variables
     * @return
     */
    public List<Protos.Environment.Variable> getList() {
        return envList;
    }

    /**
     * Adds environmental variables to the list. Please add new environmental variables here.
     * @param configuration
     */
    private void populateEnvMap(ExecutorConfig executorConfig, LogstashConfig logstashConfig) {
        addToList(native_mesos_library_key, native_mesos_library_path);
        addToList(JAVA_OPTS, getExecutorHeap(executorConfig) + " " + getLogstashHeap(logstashConfig));
    }

    private void addToList(String key, String value) {
        envList.add(getEnvProto(key, value));
    }

    private Protos.Environment.Variable getEnvProto(String key, String value) {
        return Protos.Environment.Variable.newBuilder()
                .setName(key)
                .setValue(value).build();
    }

    /**
     * @param configuration The mesos cluster configuration
     * @return A string representing the java heap space.
     */
    private String getExecutorHeap(ExecutorConfig executorConfig) {
        return "-Xmx" + executorConfig.getHeapSize() + "m";
    }
    /**
     * @param configuration The mesos cluster configuration
     * @return A string representing the java heap space.
     */
    private String getLogstashHeap(LogstashConfig logstashConfig) {
        return "-Dmesos.logstash.logstash.heap.size=" + logstashConfig.getHeapSize() + "m";
    }
}