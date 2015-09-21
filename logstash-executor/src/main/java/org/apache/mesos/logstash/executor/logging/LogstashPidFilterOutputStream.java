package org.apache.mesos.logstash.executor.logging;

import org.apache.mesos.logstash.executor.ConfigManager;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Outputstream for Logstash pid.
 */
public class LogstashPidFilterOutputStream extends FilterOutputStream {

    public static final char MAGIC_CHARACTER = '\u0002';
    private boolean ignoring = false;
    private StringBuilder pidStringBuilder = new StringBuilder();
    public String pid = null;


    public LogstashPidFilterOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    @Override
    public void write(int b) throws IOException {
        if (ignoring) {
            if (b == '\n') {
                ignoring = false;
                pid = pidStringBuilder.toString();
                ConfigManager.LOGGER.debug(String.format("Extracted logstash pid: %s", pid));
            }

            pidStringBuilder.append((char) b);

        } else {
            if (b == MAGIC_CHARACTER && pid == null) {
                ignoring = true;
            } else {
                super.write(b);
            }
        }
    }

    public String getPid() {
        return pid;
    }
}
