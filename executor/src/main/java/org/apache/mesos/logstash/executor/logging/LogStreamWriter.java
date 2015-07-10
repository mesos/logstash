package org.apache.mesos.logstash.executor.logging;

import java.io.IOException;


public interface LogStreamWriter {
    void write(String name, LogStream logStream) throws IOException;
}
