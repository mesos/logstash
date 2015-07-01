package org.apache.mesos.logstash.scheduler;

import com.google.protobuf.ServiceException;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by thb on 01/07/15.
 */
public class ConfigMonitor {

    private boolean isRunning = false;

    public ConfigMonitor(String absolutePath) {

    }

    public void run(Consumer<Map<String, String>> onChange) {
        ExecutorService runner = Executors.newSingleThreadExecutor();
        isRunning = true;
    }

    public boolean isRunning() {
        return isRunning;
    }
}
