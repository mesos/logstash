package org.apache.mesos.logstash.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;

import java.util.List;
import java.util.concurrent.ExecutionException;


public class MockPersistentState implements IPersistentState {

    @Override public Protos.FrameworkID getFrameworkID()
        throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
        return null;
    }

    @Override public void setFrameworkId(Protos.FrameworkID frameworkId)
        throws InterruptedException, ExecutionException {

    }

    @Override public boolean removeFrameworkId() throws ExecutionException, InterruptedException {
        return false;
    }

    @Override public void setLatestConfig(List<LogstashProtos.LogstashConfig> configs)
        throws ExecutionException, InterruptedException {

    }

    @Override public LogstashProtos.SchedulerMessage getLatestConfig()
        throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        return null;
    }
}
