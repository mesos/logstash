package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.config.ConfigManager;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

// TODO (thb) Ignored because they fail (timeout) when run from gradle.
// They pass when run from IDEA. hm.
@Ignore
public class ConfigManagerTest {
    private LogstashScheduler scheduler = Mockito.mock(LogstashScheduler.class);
    private Map<String, String> dockerConfig = Collections.synchronizedMap(new HashMap<>());
    private Map<String, String> hostConfig = Collections.synchronizedMap(new HashMap<>());

    @Rule
    public TemporaryFolder configDir = new TemporaryFolder();

    private AtomicInteger notificationCounter;
    private Path configPath;

    private ConfigManager monitor;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws IOException {
        notificationCounter = new AtomicInteger(0);
        configPath = configDir.getRoot().toPath();
        monitor = new ConfigManager(scheduler, configDir.getRoot().toPath());

        Mockito.<Void>doAnswer(invocation -> {
            dockerConfig.clear();
            dockerConfig.putAll(
                (Map<? extends String, ? extends String>) invocation.getArguments()[0]);

            hostConfig.clear();
            hostConfig.putAll(
                (Map<? extends String, ? extends String>) invocation.getArguments()[1]);

            notificationCounter.incrementAndGet();
            return null;
        }).when(scheduler).configUpdated(any(Map.class), any(Map.class));
    }

    @After
    public void stop() {
        monitor.stop();
    }

    public void writeConfig(String configFileName, String content) throws IOException {
        Path filePath = configPath.resolve(configFileName);
        Files.createDirectories(filePath.getParent());
        Files.write(filePath, content.getBytes());
    }

    @Test
    public void getsNotifiedOnFileCreation() throws IOException {

        monitor.start();

        await().until(monitor::isRunning);

        writeConfig("docker/my-framework.conf", "foo");

        awaitNotification();

        assertEquals(1, dockerConfig.size());
        assertEquals("foo", dockerConfig.get("my-framework"));
    }

    @Test
    public void getsNotifiedOnFileDelete() throws IOException {

        writeConfig("docker/my-framework.conf", "foo");

        monitor.start();

        await().until(monitor::isRunning);

        Files.delete(configPath.resolve("docker/my-framework.conf"));

        awaitNotification();

        assertEquals(0, dockerConfig.size());
    }

    @Test
    public void getsNotifiedOnFileUpdate() throws IOException, InterruptedException {

        // Write the config before the monitor starts.
        writeConfig("host/my-framework.conf", "foo");

        monitor.start();

        await().until(monitor::isRunning);

        // HACK: The filesystem watcher takes some time to start observing changes.
        // This is not really a major problem in prod, since it means you cannot
        // change the configs during the first few seconds of running the scheduler.
        Thread.sleep(2000);

        // Modify the config
        writeConfig("host/my-framework.conf", "bar");

        awaitNotification(2);

        assertEquals(0, dockerConfig.size());
        assertEquals(1, hostConfig.size());
        assertEquals("bar", hostConfig.get("my-framework"));
        assertTrue(hostConfig.containsKey("my-framework"));
    }

    private void awaitNotification() {
        awaitNotification(2);
    }

    private void awaitNotification(int count) {
        await().atMost(10, TimeUnit.SECONDS).untilAtomic(notificationCounter,
            Matchers.greaterThanOrEqualTo(count));
    }
}