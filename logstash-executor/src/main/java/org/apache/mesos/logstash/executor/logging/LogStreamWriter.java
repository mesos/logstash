package org.apache.mesos.logstash.executor.logging;

import java.io.IOException;

/**
 * Writer for {@link LogStream}s.
 */
public interface LogStreamWriter {
    void write(String name, LogStream logStream) throws IOException;
}
