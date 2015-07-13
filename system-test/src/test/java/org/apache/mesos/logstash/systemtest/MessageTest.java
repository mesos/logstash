package org.apache.mesos.logstash.systemtest;

import org.apache.mesos.logstash.common.LogstashProtos.ExecutorMessage;
import org.apache.mesos.mini.state.State;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class MessageTest extends AbstractLogstashFrameworkTest{


    @Before
    public void addExecutorMessageListener() {
        executorMessageListener = new ExecutorMessageListenerTestImpl();
        scheduler.addExecutorMessageListener(executorMessageListener);
    }

    @After
    public void removeExecutorMessageListener() {
        scheduler.removeAllExecutorMessageListeners();
    }


    @Test
    public void logstashTaskIsRunning() throws Exception {

        State state = cluster.getStateInfo();

        assertEquals("logstash framework should run 1 task", 1, state.getFramework("logstash").getTasks().size());
        assertEquals("LOGSTASH_SERVER", state.getFramework("logstash").getTasks().get(0).getName());
        assertEquals("TASK_RUNNING", state.getFramework("logstash").getTasks().get(0).getState());
    }


    @Test
    public void executorSendInternalStatus() throws Exception {
        List<ExecutorMessage> executorMessages = requestInternalStatusAndWaitForResponse();


        Assert.assertEquals(1, executorMessages.size());
        Assert.assertEquals("INTERNAL_STATUS", executorMessages.get(0).getType());
    }


}
