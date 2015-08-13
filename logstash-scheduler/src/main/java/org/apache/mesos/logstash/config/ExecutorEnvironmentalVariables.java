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
    public static final String JAVA_OPTS = "JAVA_OPTS";
    private final List<Protos.Environment.Variable> envList = new ArrayList<>();

    /**
     * @param configuration The mesos cluster configuration
     */
    public ExecutorEnvironmentalVariables(Configuration configuration) {
        populateEnvMap(configuration);
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
    private void populateEnvMap(Configuration configuration) {
        addToList(native_mesos_library_key, native_mesos_library_path);
        addToList(JAVA_OPTS, getExecutorHeap(configuration) + " " + getLogstashHeap(
            configuration));
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
    private String getExecutorHeap(Configuration configuration) {
        return "-Xmx" + configuration.getExecutorHeapSize() + "m";
    }
    /**
     * @param configuration The mesos cluster configuration
     * @return A string representing the java heap space.
     */
    private String getLogstashHeap(Configuration configuration) {
        return "-Dmesos.logstash.logstash.heap.size=" + configuration.getLogstashHeapSize() + "m";
    }
}