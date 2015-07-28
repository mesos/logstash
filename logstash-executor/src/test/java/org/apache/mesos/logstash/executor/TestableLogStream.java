package org.apache.mesos.logstash.executor;
import org.apache.mesos.logstash.executor.logging.LogStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class TestableLogStream implements LogStream {

    public OutputStream outputStream;
    public OutputStream stdout;
    public OutputStream stderr;
    private boolean isAttached = false;

    @Override
    public void attach(OutputStream stdout, OutputStream stderr) throws IOException {
        if (isAttached()) {
            throw new IllegalStateException("TestableLogStream already used...");
        }
        this.stderr = stderr;
        this.stdout = stdout;

        outputStream = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                stdout.write(b);
            }
        };
        isAttached = true;
    }

    boolean isAttached() {
        return isAttached;
    }

    @Override
    public void close() {

        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            stdout.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            stderr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override public String readFully() {
        throw new RuntimeException("Not implemented");
    }

    @Override public String getLogstashPid() {
        return "SOME_PID";
    }
}
