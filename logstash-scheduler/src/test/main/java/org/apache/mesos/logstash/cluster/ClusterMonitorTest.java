package org.apache.mesos.logstash.cluster;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.TestSerializableStateImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
/**
 * Created by flg on 11/08/15.
 */
public class ClusterMonitorTest {

    public static final String SOME_FRAMEWORK_ID = "SOME_FRAMEWORK_ID";
    public static final String SOME_EXECUTOR_ID = "SOME_EXECUTOR_ID";
    public static final String SOME_SLAVE_ID = "SOME_SLAVE_ID";
    public static final String SOME_TASK_ID = "SOME_TASK_ID";
    ClusterMonitor clusterMonitor;
    ClusterMonitor.ReconcileSchedule reconcileSchedule;

    @Before
    public void setup(){
        reconcileSchedule = mock(ClusterMonitor.ReconcileSchedule.class);
        Configuration configuration = new Configuration();
        TestSerializableStateImpl state = new TestSerializableStateImpl();
        FrameworkState frameworkState = new FrameworkState(state);
        frameworkState.setFrameworkId(createFrameworkId(SOME_FRAMEWORK_ID));
        ClusterState clusterState = new ClusterState(state, frameworkState);
        LiveState liveState = new LiveState();

        clusterMonitor = new ClusterMonitor(configuration, clusterState, liveState);
        clusterMonitor.reconcileSchedule = reconcileSchedule;
    }

    @Test
    public void testGetExecutionPhase_intiallyShouldBeInReconciliation() throws Exception {
        assertEquals(ClusterMonitor.ExecutionPhase.RECONCILING_TASKS, clusterMonitor.getExecutionPhase());
    }

    @Test
    public void testGetRunningTasks_withNoPersistedTask_shouldReturnEmptyList() throws Exception {
        assertEquals(0, clusterMonitor.getRunningTasks().size());
    }

    @Test
    public void testUpdateTask_withUnknownRunningTask_shouldDoNothingBecauseItIsUnknown() throws Exception {
        clusterMonitor.update(null, createTaskStatus(Protos.TaskState.TASK_RUNNING, SOME_TASK_ID));

        assertEquals(0, clusterMonitor.getRunningTasks().size());
    }

    private Protos.TaskStatus createTaskStatus(Protos.TaskState taskState, String taskId) {
        return Protos.TaskStatus.newBuilder()
            .setState(taskState)
            .setTaskId(Protos.TaskID.newBuilder().setValue(taskId))
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue(SOME_EXECUTOR_ID).build())
            .setSlaveId(Protos.SlaveID.newBuilder().setValue(SOME_SLAVE_ID)).build();
    }

    @Test
    public void testStartReconciling() throws Exception {

    }

    @Test
    public void testStopReconciling() throws Exception {

    }

    @Test
    public void testIsReconciling() throws Exception {

    }

    private Protos.FrameworkID createFrameworkId(String frameworkId) {
        return Protos.FrameworkID.newBuilder().setValue(frameworkId).build();
    }
}