package org.apache.mesos.logstash.executor;

import org.apache.commons.io.FileUtils;
import org.apache.mesos.logstash.executor.logging.FileLogSteamWriter;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

public class LogStreamTest {

    public static final int MAX_LOG_SIZE = 11;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    File testLogFile;
    FileLogSteamWriter writer;
    TestableLogStream testableLogStream;

    @Test
    public void simpleRotation() throws IOException {
        String testString = "123456789ABCDEFGH";
        testableLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("CDEFGH", FileUtils.readFileToString(testLogFile));
    }

    @Test
    public void rotatesMultipleTimesIfNecessary() throws IOException {
        String testString = "123456789ABCDEFGHIJKLMNOPQRSTUV";
        testableLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("NOPQRSTUV", FileUtils.readFileToString(testLogFile));
    }

    @Test
    public void noRotationUntilLimitIsReached() throws IOException {
        String testString = "1234";
        testableLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("1234", FileUtils.readFileToString(testLogFile));
    }

    @Test
    public void emptyContentJustAfterRotation() throws IOException {
        String testString = "123456789AB";
        testableLogStream.outputStream.write(testString.getBytes("UTF-8"));

        Assert.assertEquals("", FileUtils.readFileToString(testLogFile));
    }

    // Our log rotation works on byte level, so is completely oblivious to character encodings
    @Test
    public void cutsMultibyteUnicodeCharactersInHalf() throws IOException {
        String testString = "        Fl\u00f6r\u00fcan";
        testableLogStream.outputStream.write(testString.getBytes("UTF-8"));
        String expected = "\ufffdr\u00fcan";
        Assert.assertEquals(expected, FileUtils.readFileToString(testLogFile, "UTF-8"));
    }

    @Before
    public void setup() throws IOException {
        try {
            testLogFile = folder.newFile("testLog.log");
            writer = new FileLogSteamWriter(MAX_LOG_SIZE);

            testableLogStream = new TestableLogStream();

            writer.write(testLogFile.getAbsolutePath(), testableLogStream);
            waitForTestLogStreamIsAttached(testableLogStream);
        } catch (Throwable e) {
            if (testableLogStream != null && testableLogStream.isAttached()) {
                testableLogStream.close();
            }
        }
    }

    @After
    public void tearDown() throws IOException {
        if (testableLogStream != null && testableLogStream.isAttached()) {
            testableLogStream.close();
        }
    }

    private void waitForTestLogStreamIsAttached(final TestableLogStream testableLogStream) {
        await("Waiting for attached LogStream")
            .atMost(5, TimeUnit.SECONDS)
            .until(testableLogStream::isAttached);
    }

}