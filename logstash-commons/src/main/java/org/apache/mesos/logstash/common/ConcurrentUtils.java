package org.apache.mesos.logstash.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utils for concurrency.
 */
public final class ConcurrentUtils {
    protected ConcurrentUtils() {
    }

    public static void stop(ExecutorService executor) {
        stop(executor, 5);
    }

    public static void stop(ExecutorService executor, int timeoutSeconds) {
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        } finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            System.out.println("shutdown finished");
        }
    }

    public static <K, V> Map<K, V> newSyncronizedHashMap() {
        return Collections.synchronizedMap(new HashMap<>());
    }
}
