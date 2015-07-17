package org.apache.mesos.logstash.scheduler;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Awaitility.fieldIn;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

@Ignore
public class ConfigMonitorTest {
    @Rule
    public TemporaryFolder configDir = new TemporaryFolder();

    AtomicInteger notificationCounter;

    @Before
    public void setup() {
        // Start at -1 because we want to ignore the initial notification from ConfigMonitor
        notificationCounter = new AtomicInteger(-1);
    }

    public void awaitRunning(ConfigMonitor monitor) {
        await().until(fieldIn(monitor).ofType(boolean.class).andWithName("isRunning"), is(true));
    }

    public void writeConfig(String configFileName, String content) throws IOException {
        FileUtils.write(getFilePath(configFileName), content);
        FileUtils.touch(getFilePath(configFileName));
    }

    private File getFilePath(String configFileName) {
        return new File(configDir.getRoot() + "/" + configFileName);
    }

    @Test
    public void getsNotifiedOnFileCreation() throws IOException {
        // Setup
        ConfigMonitor monitor = new ConfigMonitor(configDir.getRoot().getAbsolutePath());

        final Map<String, String> config = startMonitor(monitor);

        // Execute
        writeConfig("my-framework.conf", "foo");

        System.out.println("Awaiting notification");
        awaitNotification(monitor);

        // Verify
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

        assertEquals(0, config.size());
    }


    @Test
    public void getsNotifiedOnFileUpdate() throws IOException {
        ConfigMonitor monitor = new ConfigMonitor(configDir.getRoot().getAbsolutePath());

        // Write the config before the monitor starts.
        writeConfig("my-framework.conf", "foo");

        final Map<String, String> config = startMonitor(monitor);

        // Work around because file system watcher on mac is slow
        // and doesn't notice writes if they happen too fast after each other
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Modify the config
        writeConfig("my-framework.conf", "bar");

        awaitNotification(monitor);

        assertEquals(1, config.size());
    }

    private void awaitNotification(ConfigMonitor monitor) {
        await().untilAtomic(notificationCounter, is(1));
    }

    private Map<String, String> startMonitor(ConfigMonitor monitor) {
        final Map<String, String> config = new ConcurrentHashMap<>();

        monitor.start();

        awaitRunning(monitor);

        return config;
    }
}