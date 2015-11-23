package org.apache.mesos.logstash.executor.logging;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Outputstream for Logstash pid.
 * Behavior is to:
 *   - filter out all substrings that are delimited by the start character MAGIC_CHARACTER
 *     and the end character '\n'.
 *   - identify the first such substring which is parseable as an integer,
 *     log it, and make it available as `getPid()`.
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
                ConfigManager.LOGGER.debug("Extracted logstash pid: {}", pid);
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
