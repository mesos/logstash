package org.apache.mesos.logstash.cluster;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.cluster.ClusterMonitor.ExecutionPhase;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.FrameworkConfig;
import org.apache.mesos.logstash.scheduler.Task;
import org.apache.mesos.logstash.state.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TimerTask;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.mesos.Protos.TaskState.TASK_LOST;
import static org.apache.mesos.Protos.TaskState.TASK_RUNNING;
import static org.apache.mesos.logstash.util.ProtoTestUtil.createTaskInfo;
import static org.apache.mesos.logstash.util.ProtoTestUtil.createTaskStatus;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ClusterMonitorTest {

    private static final String SOME_FRAMEWORK_ID = "SOME_FRAMEWORK_ID";
    private static final String SOME_EXECUTOR_ID = "SOME_EXECUTOR_ID";
    private static final String SOME_SLAVE_ID = "SOME_SLAVE_ID";
    private static final String SOME_TASK_ID_1 = "SOME_TASK_ID_1";
    private static final String SOME_TASK_ID_2 = "SOME_TASK_ID_2";
    @InjectMocks
    private ClusterMonitor clusterMonitor = new ClusterMonitor();

    private FrameworkConfig frameworkConfig = new FrameworkConfig();

    @Mock
    private ClusterState clusterState;
    @Mock
    private LiveState liveState;

    @Mock
    StatePath statePath;

    @Mock
    private ClusterMonitor.ReconcileSchedule reconcileScheduleMock;

    @Mock
    private SchedulerDriver driver;

    @Captor
    private ArgumentCaptor<TimerTask> timerTaskArgumentCaptor;

    @Captor
    private ArgumentCaptor<Collection<TaskStatus>> taskStatusArgumentCaptor;

    @Mock
    private FrameworkState frameworkState;

    @Mock
    SerializableState state;

    @Before
    public void setup() {
        frameworkConfig.setFrameworkName("SOME_FRAMEWORK_NAME");
        clusterMonitor.frameworkConfig = frameworkConfig;

//        FrameworkState frameworkState = new FrameworkState(state);
//        frameworkState.setFrameworkId(createFrameworkId(SOME_FRAMEWORK_ID));
//        configuration.setState(state);
    }

    @Test
    public void testGetExecutionPhase_initiallyShouldBeInReconciliation() throws Exception {
        assertEquals(ExecutionPhase.RECONCILING_TASKS, clusterMonitor.getExecutionPhase());
    }

    @Test
    public void testGetRunningTasks_withNoPersistedTask_shouldReturnEmptyList() throws Exception {
        assertEquals(0, clusterMonitor.getRunningTasks().size());
    }

    @Test
    public void testUpdateTask_withUnknownRunningTask_shouldDoNothingBecauseItIsUnknown()
        throws Exception {
        clusterMonitor = new ClusterMonitor();

        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_1, SOME_SLAVE_ID));

        assertEquals(0, clusterMonitor.getRunningTasks().size());
        assertEquals(0, liveState.getNonTerminalTasks().size());
    }

    @Test
    public void testUpdateTask_withKnownAndUnknownRunningTask_shouldProcessOnlyKnownOnes()
        throws Exception {
        TaskInfo taskInfo = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        clusterState.addTask(taskInfo);

        TaskStatus taskStatus = createTaskStatus(TASK_RUNNING, SOME_TASK_ID_1,
            SOME_SLAVE_ID);
        TaskStatus taskStatus_unknownTask = createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_2, SOME_SLAVE_ID);

        clusterMonitor = new ClusterMonitor();
        clusterMonitor.update(null, taskStatus);
        clusterMonitor.update(null, taskStatus_unknownTask);

        assertEquals(1, clusterMonitor.getRunningTasks().size());
        assertEquals(SOME_TASK_ID_1,
            clusterMonitor.getRunningTasks().get(0).getTaskId().getValue());
        assertEquals(1, liveState.getNonTerminalTasks().size());
        Task liveStateTask = liveState.getNonTerminalTasks().iterator().next();
        assertEquals(SOME_TASK_ID_1, liveStateTask.getTaskId().getValue());
    }

    @Test
    public void testUpdateTask_withTerminalTask_shouldRemoveFromRunningTasks()
        throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskInfo taskInfo2 = createTaskInfo(SOME_TASK_ID_2, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskStatus taskStatus1 = createTaskStatus(TASK_RUNNING,
            SOME_TASK_ID_1, SOME_SLAVE_ID);
        TaskStatus taskStatus2 = createTaskStatus(TASK_LOST,
            SOME_TASK_ID_2, SOME_SLAVE_ID);

        clusterState.addTask(taskInfo1);
        clusterState.addTask(taskInfo2);
        clusterMonitor = new ClusterMonitor();

        clusterMonitor.update(null, taskStatus1);
        clusterMonitor.update(null, taskStatus2);

        assertEquals(1, clusterMonitor.getRunningTasks().size());
        assertEquals(SOME_TASK_ID_1,
            clusterMonitor.getRunningTasks().get(0).getTaskId().getValue());
    }

    @Test
    public void testUpdateTask_withStillNonTerminal_shouldJustUpdateTask() throws Exception {
        TaskInfo taskInfo = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);

        when(clusterState.getTaskList()).thenReturn(Collections.singletonList(taskInfo));
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));
        clusterMonitor.afterPropertiesSet();

        LSTaskStatus executorState = mock(LSTaskStatus.class);

        when(clusterState.getStatus(taskInfo.getTaskId())).thenReturn(executorState);
        when(executorState.isRunning()).thenReturn(true);
        when(executorState.getTaskInfo()).thenReturn(taskInfo);

        String message = "SOME INTERESTING MESSAGE - EVEN IF WE DO NOT USE THE MESSAGE FIELD...";
        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING, SOME_TASK_ID_1, SOME_SLAVE_ID, message));

        List<TaskInfo> runningTasks = clusterMonitor.getRunningTasks();
        assertEquals(1, runningTasks.size());
        assertEquals(SOME_TASK_ID_1, runningTasks.get(0).getTaskId().getValue());
    }

    @Test
    public void testStartReconciling_withoutAnyPersitedTasks_shouldFinishReconciliation()
        throws Exception {
        clusterMonitor = new ClusterMonitor();
        clusterMonitor.reconcileSchedule = reconcileScheduleMock;

        clusterMonitor.startReconciling(driver);

        assertEquals(ExecutionPhase.RECONCILIATION_DONE, clusterMonitor.getExecutionPhase());
        verifyZeroInteractions(reconcileScheduleMock);
    }

    @Test
    public void testStartReconciling_withSomePersistedTasks_shouldCallDriverReconcileTasks()
        throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskInfo taskInfo2 = createTaskInfo(SOME_TASK_ID_2, SOME_EXECUTOR_ID, SOME_SLAVE_ID);

        clusterState.addTask(taskInfo1);
        clusterState.addTask(taskInfo2);
        clusterMonitor = new ClusterMonitor();
        clusterMonitor.reconcileSchedule = reconcileScheduleMock;

        clusterMonitor.startReconciling(driver);

        verify(driver, times(1)).reconcileTasks(taskStatusArgumentCaptor.capture());

        Collection<TaskStatus> tasksToReconcile = taskStatusArgumentCaptor.getValue();
        assertEquals(2, tasksToReconcile.size());

        assertThat(
            tasksToReconcile.stream().map(o -> o.getTaskId().getValue())
                .collect(Collectors.toSet()),
            containsInAnyOrder(SOME_TASK_ID_1, SOME_TASK_ID_2));
    }

    @Test
    public void testStartReconciling_withSomePersistedTasks_shouldScheduleTimerTask()
        throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);

        clusterState.addTask(taskInfo1);
        clusterMonitor = new ClusterMonitor();
        clusterMonitor.reconcileSchedule = reconcileScheduleMock;

        clusterMonitor.startReconciling(driver);

        verify(reconcileScheduleMock, times(1)).schedule(any(TimerTask.class), eq(1));
    }

    @Test
    public void testStartReconciling_withSomePersistedTasksNeverGetReconciled_shouldGiveUpAndFinishReconciliation()
        throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskInfo taskInfo2 = createTaskInfo(SOME_TASK_ID_2, SOME_EXECUTOR_ID, SOME_SLAVE_ID);

        clusterState.addTask(taskInfo1);
        clusterState.addTask(taskInfo2);
        clusterMonitor = new ClusterMonitor();
        clusterMonitor.reconcileSchedule = reconcileScheduleMock;

        clusterMonitor.startReconciling(driver);


        /*
         * Now we simulate the reconciliation retries which are triggered after a increasing timeout when still
         * some of our persisted tasks haven't received a status update yet.
         * When reaching the MAX_RETRY we give up and remove the persisted tasks and go on...
         */
        for (int i = 0; i <= ClusterMonitor.ReconcileStateTask.MAX_RETRY; i++) {
            verify(reconcileScheduleMock, times(1)).schedule(timerTaskArgumentCaptor.capture(),
                anyInt());
            reset(reconcileScheduleMock);
            timerTaskArgumentCaptor.getValue().run();
        }

        // we're not scheduling another task when the limit is reached
        verifyZeroInteractions(reconcileScheduleMock);

        assertEquals(ExecutionPhase.RECONCILIATION_DONE, clusterMonitor.getExecutionPhase());
        assertEquals(0, clusterMonitor.getRunningTasks()
            .size()); // we removed all known remaing running tasks because we haven't received a status update

    }

    @Test
    public void testStartReconciling_withSomePersistedTasksGetReconciledAfterSomeRetriesLater_shouldFinishReconciliation()
        throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskInfo taskInfo2 = createTaskInfo(SOME_TASK_ID_2, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskStatus taskStatus1 = createTaskStatus(TASK_RUNNING, SOME_TASK_ID_1, SOME_SLAVE_ID);

        clusterState.addTask(taskInfo1);
        clusterState.addTask(taskInfo2);
        clusterMonitor = new ClusterMonitor();
        clusterMonitor.reconcileSchedule = reconcileScheduleMock;

        clusterMonitor.startReconciling(driver);

        clusterMonitor.update(null, taskStatus1); // simulate a status update

        // manually call the timer task
        verify(reconcileScheduleMock, times(1)).schedule(timerTaskArgumentCaptor.capture(),
            anyInt());
        reset(driver);
        reset(reconcileScheduleMock);
        timerTaskArgumentCaptor.getValue().run();


        verify(driver, times(1)).reconcileTasks(taskStatusArgumentCaptor.capture());
        Collection<TaskStatus> tasksToReconcile = taskStatusArgumentCaptor.getValue();
        assertEquals(1, tasksToReconcile.size());

        // make sure that we request the status update only for the remaining tasks
        assertThat(
            tasksToReconcile.stream().map(o -> o.getTaskId().getValue())
                .collect(Collectors.toSet()),
            containsInAnyOrder(SOME_TASK_ID_2));

        /*
         * Now we simulate the remaining reconciliation retries which are triggered after a increasing timeout when still
         * some of our persisted tasks haven't received a status update yet.
         * When reaching the MAX_RETRY we give up and remove the persisted tasks and go on...
         */
        for (int i = 1; i <= ClusterMonitor.ReconcileStateTask.MAX_RETRY; i++) {
            verify(reconcileScheduleMock, times(1)).schedule(timerTaskArgumentCaptor.capture(),
                anyInt());
            reset(reconcileScheduleMock);
            timerTaskArgumentCaptor.getValue().run();
        }

        // we're not scheduling another task when the limit is reached
        verifyZeroInteractions(reconcileScheduleMock);

        assertEquals(ExecutionPhase.RECONCILIATION_DONE, clusterMonitor.getExecutionPhase());
        assertEquals(1, clusterMonitor.getRunningTasks()
            .size()); // we removed all known remaing running tasks because we haven't received a status update
        assertEquals(SOME_TASK_ID_1,
            clusterMonitor.getRunningTasks().iterator().next().getTaskId().getValue());
    }

    @Test
    public void testStartReconciling_withAllPersistedTasksGetReconciledAfterSomeRetriesLater_shouldFinishReconciliation() throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        TaskInfo taskInfo2 = createTaskInfo(SOME_TASK_ID_2, SOME_EXECUTOR_ID, SOME_SLAVE_ID);

        when(clusterState.getTaskList()).thenReturn(asList(taskInfo1, taskInfo2));
        when(clusterState.getStatus(taskInfo1.getTaskId())).thenReturn()
        when(frameworkState.getFrameworkID()).thenReturn(createFrameworkId("test"));
        clusterMonitor.afterPropertiesSet();

        clusterMonitor.reconcileSchedule = reconcileScheduleMock;

        when(clusterState.getTaskIdList()).thenReturn(asList(taskInfo1.getTaskId(), taskInfo2.getTaskId()));
        clusterMonitor.startReconciling(driver);
        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING, SOME_TASK_ID_1, SOME_SLAVE_ID)); // simulate a status update

        // manually call the timer task
        verify(reconcileScheduleMock).schedule(timerTaskArgumentCaptor.capture(), anyInt());
        reset(driver);
        reset(reconcileScheduleMock);
        timerTaskArgumentCaptor.getValue().run();


        verify(driver, times(1)).reconcileTasks(taskStatusArgumentCaptor.capture());
        Collection<TaskStatus> tasksToReconcile = taskStatusArgumentCaptor.getValue();
        assertEquals(1, tasksToReconcile.size());

        // make sure that we request the status update only for the remaining tasks
        assertThat(
            tasksToReconcile.stream().map(o -> o.getTaskId().getValue())
                .collect(Collectors.toSet()),
            containsInAnyOrder(SOME_TASK_ID_2));


        clusterMonitor.update(null, createTaskStatus(TASK_RUNNING, SOME_TASK_ID_2, SOME_SLAVE_ID)); // simulate a status update

        verify(reconcileScheduleMock, times(1)).schedule(timerTaskArgumentCaptor.capture(),
            anyInt());
        reset(driver);
        reset(reconcileScheduleMock);
        timerTaskArgumentCaptor.getValue().run();


        // we're not scheduling another task when the limit is reached
        verifyZeroInteractions(reconcileScheduleMock);
        verifyZeroInteractions(driver);

        assertEquals(ExecutionPhase.RECONCILIATION_DONE, clusterMonitor.getExecutionPhase());
        assertEquals(2, clusterMonitor.getRunningTasks().size()); // we removed all known remaing running tasks because we haven't received a status update
    }

    @Test
    public void testStopReconciling() throws Exception {
        TaskInfo taskInfo1 = createTaskInfo(SOME_TASK_ID_1, SOME_EXECUTOR_ID, SOME_SLAVE_ID);
        clusterState.addTask(taskInfo1);

        clusterMonitor = new ClusterMonitor();
        assertEquals(ExecutionPhase.RECONCILING_TASKS, clusterMonitor.getExecutionPhase());

        clusterMonitor.stopReconciling();
        assertEquals(ExecutionPhase.RECONCILIATION_DONE, clusterMonitor.getExecutionPhase());

    }


    private Protos.FrameworkID createFrameworkId(String frameworkId) {
        return Protos.FrameworkID.newBuilder().setValue(frameworkId).build();
    }
}