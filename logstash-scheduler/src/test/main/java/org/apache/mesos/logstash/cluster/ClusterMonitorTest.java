package org.apache.mesos.logstash.cluster;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.state.ClusterState;
import org.apache.mesos.logstash.state.FrameworkState;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.TestSerializableStateImpl;
import org.apache.mesos.logstash.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.mesos.Protos.TaskState.TASK_LOST;
import static org.apache.mesos.Protos.TaskState.TASK_RUNNING;
import static org.apache.mesos.logstash.util.ProtoTestUtil.createTaskInfo;
import static org.apache.mesos.logstash.util.ProtoTestUtil.createTaskStatus;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
/**
 * Created by flg on 11/08/15.
 */
public class ClusterMonitorTest {

    public static final String SOME_FRAMEWORK_ID = "SOME_FRAMEWORK_ID";
    public static final String SOME_EXECUTOR_ID = "SOME_EXECUTOR_ID";
    public static final String SOME_SLAVE_ID = "SOME_SLAVE_ID";
    public static final String SOME_TASK_ID_1 = "SOME_TASK_ID_1";
    public static final String SOME_TASK_ID_2 = "SOME_TASK_ID_2";
    ClusterMonitor clusterMonitor;
    ClusterMonitor.ReconcileSchedule reconcileSchedule;

    @Before
    public void setup() {
        reconcileSchedule = mock(ClusterMonitor.ReconcileSchedule.class);
        Configuration configuration = new Configuration();
        configuration.setFrameworkName("SOME_FRAMEWORK_NAME");

        TestSerializableStateImpl state = new TestSerializableStateImpl();
        FrameworkState frameworkState = new FrameworkState(state);
        frameworkState.setFrameworkId(createFrameworkId(SOME_FRAMEWORK_ID));
        configuration.setFrameworkState(frameworkState);
        configuration.setState(state);

        ClusterState clusterState = new ClusterState(state, frameworkState);
        LiveState liveState = new LiveState();

        clusterMonitor = new ClusterMonitor(configuration, clusterState, liveState);
        clusterMonitor.reconcileSchedule = reconcileSchedule;
    }

    @Test
    public void testGetExecutionPhase_intiallyShouldBeInReconciliation() throws Exception {
        assertEquals(ClusterMonitor.ExecutionPhase.RECONCILING_TASKS,
            clusterMonitor.getExecutionPhase());
    }

    @Test
    public void testGetRunningTasks_withNoPersistedTask_shouldReturnEmptyList() throws Exception {
        assertEquals(0, clusterMonitor.getRunningTasks().size());
    }

    @Test
    public void testUpdateTask_withUnknownRunningTask_shouldDoNothingBecauseItIsUnknown()
        throws Exception {
        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_1, SOME_SLAVE_ID));

        assertEquals(0, clusterMonitor.getRunningTasks().size());
    }

    @Test
    public void testUpdateTask_withKnownRunningTask_shouldDoNothingBecauseItIsUnknown()
        throws Exception {
        clusterMonitor.monitorTask(
            createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID));

        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_1, SOME_SLAVE_ID));
        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_2, SOME_SLAVE_ID));

        assertEquals(1, clusterMonitor.getRunningTasks().size());
        assertEquals(SOME_TASK_ID_1, clusterMonitor.getRunningTasks().get(0).getTaskId().getValue());
    }

    @Test
    public void testUpdateTask_withTerminalTask_shouldRemoveFromRunningTasks()
        throws Exception {
        clusterMonitor.monitorTask(
            createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID));
        clusterMonitor.monitorTask(
            createTaskInfo(SOME_TASK_ID_2, SOME_EXECUTOR_ID, SOME_SLAVE_ID));

        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_1, SOME_SLAVE_ID));
        clusterMonitor.update(null, createTaskStatus(TASK_LOST,
            SOME_TASK_ID_2, SOME_SLAVE_ID));

        assertEquals(1, clusterMonitor.getRunningTasks().size());
        assertEquals(SOME_TASK_ID_1, clusterMonitor.getRunningTasks().get(0).getTaskId().getValue());
    }

    @Test
    public void testUpdateTask_withStillNonTerminal_shouldJustUpdateTask()
        throws Exception {
        clusterMonitor.getClusterState().addTask(
            createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID));

        String message = "SOME INTERESTING MESSAGE - EVEN IF WE DO NOT USE THE MESSAGE FIELD...";
        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_1, SOME_SLAVE_ID,
            message));

        assertEquals(1, clusterMonitor.getRunningTasks().size());
        assertEquals(SOME_TASK_ID_1, clusterMonitor.getRunningTasks().get(0).getTaskId().getValue());
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