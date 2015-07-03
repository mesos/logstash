package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.logging.HeartbeatFilterOutputStream;
import org.apache.mesos.logstash.logging.LogDispatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.*;

public class HeartbeatFilterOutputStreamTest {
    final String TEST_STRING = String.format("Hello\n%c Ignore me\nWorld!", LogDispatcher.MAGIC_CHARACTER);


    ByteArrayOutputStream baos;
    HeartbeatFilterOutputStream target;

    @Before
    public void setUp() throws Exception {
        baos = new ByteArrayOutputStream();
        target = new HeartbeatFilterOutputStream(baos);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void magicCharacterIsOneByteUTF8() throws Exception {
        final String singleByteString = LogDispatcher.MAGIC_CHARACTER + "";
        assertEquals(1, singleByteString.getBytes("UTF-8").length);
    }

    @Test
    public void testWorksAsUsualWithoutMagicCharacter() throws Exception {
        final String TEST_STRING = "Hello\nWorld!";

        target.write(TEST_STRING.getBytes("UTF-8"), 3, 5);
        target.write('a');
        target.write('b');

        baos.close();
        String result = baos.toString("UTF-8");

        assertEquals("lo\nWoab", result);
    }

    @Test
    public void ignoresMagicCharacterWhenInvokingMultipleTimes() throws Exception {

        target.write('a');
        target.write(LogDispatcher.MAGIC_CHARACTER);
        target.write('a');
        target.write('a');
        target.write('\n');
        target.write('b');

        baos.close();
        String result = baos.toString("UTF-8");

        assertEquals("ab", result);
    }

    @Test
    public void ignoresMagicCharacter() throws Exception {
        target.write(TEST_STRING.getBytes("UTF-8"));
        baos.close();
        String result = baos.toString("UTF-8");

        assertEquals("Hello\nWorld!", result);
    }

    @Test
    public void ignoresMagicCharacter2() throws Exception {
        target.write(TEST_STRING.getBytes("UTF-8"), 3, 4);
        baos.close();
        String result = baos.toString("UTF-8");

        assertEquals(3, baos.size());
        assertEquals("lo\n", result);
    }
}