package org.apache.mesos.logstash.executor;

import org.apache.commons.io.FileUtils;
import org.apache.mesos.logstash.executor.logging.FileLogSteamWriter;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

public class LogStreamTest {

    public static final int MAX_LOG_SIZE = 11;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    File testLogFile;
    FileLogSteamWriter writer;
    TestLogStream testLogStream;

    @Test
    public void simpleRotation() throws IOException {
        String testString = "123456789ABCDEFGH";
        testLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("CDEFGH", FileUtils.readFileToString(testLogFile));
    }

    @Test
    public void rotatesMultipleTimesIfNecessary() throws IOException {
        String testString = "123456789ABCDEFGHIJKLMNOPQRSTUV";
        testLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("NOPQRSTUV", FileUtils.readFileToString(testLogFile));
    }

    @Test
    public void noRotationUntilLimitIsReached() throws IOException {
        String testString = "1234";
        testLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("1234", FileUtils.readFileToString(testLogFile));
    }

    @Test
    public void emptyContentJustAfterRotation() throws IOException {
        String testString = "123456789AB";
        testLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("", FileUtils.readFileToString(testLogFile));
    }

    // Our log rotation works on byte level, so is completely oblivious to character encodings
    @Test
    public void cutsMultibyteUnicodeCharactersInHalf() throws IOException {
        String testString = "        Flörüan";
        testLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("�rüan", FileUtils.readFileToString(testLogFile));
    }

    @Before
    public void setup() throws IOException {
        try {
            testLogFile = folder.newFile("testLog.log");
            writer = new FileLogSteamWriter(MAX_LOG_SIZE);

            testLogStream = new TestLogStream();

            writer.write(testLogFile.getAbsolutePath(), testLogStream);
            waitForTestLogStreamIsAttached(testLogStream);
        } catch (Throwable e) {
            if (testLogStream != null && testLogStream.isAttached()) {
                testLogStream.close();
            }
        }
    }

    @After
    public void tearDown() throws IOException {
        if (testLogStream != null && testLogStream.isAttached()) {
            testLogStream.close();
        }
    }

    private void waitForTestLogStreamIsAttached(final TestLogStream testLogStream) {
        await("Waiting for attached LogStream")
            .atMost(5, TimeUnit.SECONDS)
            .until(testLogStream::isAttached);
    }

    static class TestLogStream implements LogStream, Closeable {

        OutputStream outputStream;
        OutputStream stdout;
        OutputStream stderr;

        @Override
        public void attach(OutputStream stdout, OutputStream stderr) throws IOException {
            if (isAttached()) {
                throw new IllegalStateException("TestLogStream already used...");
            }
            this.stderr = stderr;
            this.stdout = stdout;

            outputStream = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    stdout.write(b);
                }
            };
        }

        boolean isAttached() {
            return outputStream != null;
        }

        @Override
        public void close() throws IOException {
            if (outputStream != null) {
                outputStream.close();
                outputStream = null;
            }
            stdout.close();
            stderr.close();
        }
    }

}