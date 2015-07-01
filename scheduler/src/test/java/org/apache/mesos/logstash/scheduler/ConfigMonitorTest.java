package org.apache.mesos.logstash.scheduler;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Awaitility.fieldIn;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;


public class ConfigMonitorTest {
    @Rule
    public TemporaryFolder configDir = new TemporaryFolder();

    AtomicBoolean done;

    @Before
    public void setup() {
        System.out.println("STARTING UP");
        done = new AtomicBoolean(false);
    }

    public void awaitRunning(ConfigMonitor monitor) {
        await().until(fieldIn(monitor).ofType(boolean.class).andWithName("isRunning"), is(true));
    }

    public void writeConfig(String configFileName, String content) throws IOException {
        FileUtils.write(getFilePath(configFileName), content);
    }

    private File getFilePath(String configFileName) {
        return new File(configDir.getRoot() + "/" + configFileName);
    }

    @Test
    public void getsNotifiedOnFileCreation() throws IOException {
        ConfigMonitor monitor = new ConfigMonitor(configDir.getRoot().getAbsolutePath());

        final Map<String, String> config = startMonitor(monitor);

        writeConfig("my-framework.conf", "foo");

        awaitNotification(monitor);

        System.out.println("ASKING");

        assertEquals(1, config.size());
        assertEquals("foo", config.get("my-framework"));
    }


    @Test
    public void getsNotifiedOnFileDelete() throws IOException {
        ConfigMonitor monitor = new ConfigMonitor(configDir.getRoot().getAbsolutePath());

        writeConfig("my-framework.conf", "foo");

        final Map<String, String> config = startMonitor(monitor);

        FileUtils.forceDelete(getFilePath("my-framework.conf"));

        awaitNotification(monitor);

        assertEquals(config.size(), 0);
    }


    @Test
    public void getsNotifiedOnFileUpdate() throws IOException {
        ConfigMonitor monitor = new ConfigMonitor(configDir.getRoot().getAbsolutePath());

        // Write the config before the monitor starts.

        writeConfig("my-framework.conf", "foo");

        final Map<String, String> config = startMonitor(monitor);

        // Update the config.

        writeConfig("my-framework.conf", "bar");

        awaitNotification(monitor);

        assertEquals(config.size(), 0);
    }

    private void awaitNotification(ConfigMonitor monitor) {
//        try {
//            monitor.getThread().wait(3_000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        await().untilTrue(done);
    }

    private Map<String, String> startMonitor(ConfigMonitor monitor) {
        final Map<String, String> config = new ConcurrentHashMap<>();
        monitor.start(c -> {
            config.putAll(c);
            done.set(true);
        });


        done.set(false);
        awaitRunning(monitor);

        return config;
    }
}