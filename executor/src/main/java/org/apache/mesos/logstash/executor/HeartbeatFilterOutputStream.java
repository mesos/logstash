package org.apache.mesos.logstash.executor;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by peldan on 25/06/15.
 */
public class HeartbeatFilterOutputStream extends FilterOutputStream {

    private boolean ignoring = false;

    public HeartbeatFilterOutputStream(OutputStream s) {
        super(s);
    }

    @Override
    public void write(int b) throws IOException {
        if(ignoring) {
            if(b == '\n') {
                ignoring = false;
            }
        }
        else {
            if(b == LogDispatcher.MAGIC_CHARACTER) {
                ignoring = true;
            }
            else {
                super.write(b);
            }
        }
    }
}
