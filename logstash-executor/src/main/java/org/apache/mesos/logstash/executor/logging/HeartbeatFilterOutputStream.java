package org.apache.mesos.logstash.executor.logging;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class HeartbeatFilterOutputStream extends FilterOutputStream {

    public static final char MAGIC_CHARACTER = '\u0002';
    private boolean ignoring = false;

    public HeartbeatFilterOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    @Override
    public void write(int b) throws IOException {
        if (ignoring) {
            if (b == '\n') {
                ignoring = false;
            }
        } else {
            if (b == MAGIC_CHARACTER) {
                ignoring = true;
            } else {
                super.write(b);
            }
        }
    }
}
