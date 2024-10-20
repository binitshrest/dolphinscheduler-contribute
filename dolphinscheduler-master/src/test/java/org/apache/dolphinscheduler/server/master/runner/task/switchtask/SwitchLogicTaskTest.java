package org.apache.dolphinscheduler.server.master.runner.task.switchtask;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.model.SwitchResultVo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.SwitchParameters;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.MasterTaskExecuteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SwitchLogicTaskTest {

    @Mock
    private IWorkflowExecutionRunnable workflowExecutionRunnable;

    @Mock
    private TaskExecutionContext taskExecutionContext;

    @InjectMocks
    private SwitchLogicTask switchLogicTask;

    private SwitchParameters switchParameters;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Mock taskExecutionContext and workflowExecutionRunnable
        switchParameters = new SwitchParameters();
        when(taskExecutionContext.getTaskParams()).thenReturn("{}"); // mock task params
        when(taskExecutionContext.getPrepareParamsMap()).thenReturn(new HashMap<>());
    }

    @Test
    void testHandle_WithMatchingBranch() throws MasterTaskExecuteException {
        // Set up switch parameters with matching branch condition
        List<SwitchResultVo> switchResultVoList = new ArrayList<>();
        SwitchResultVo switchResultVo = new SwitchResultVo();
        switchResultVo.setCondition("true"); // simple condition that matches
        switchResultVo.setNextNode(123L); // mock next branch node
        switchResultVoList.add(switchResultVo);

        switchParameters.setSwitchResult(new SwitchParameters.SwitchResult(switchResultVoList, 999L));

        // Mock the task instance and context setup
        Map<String, Property> globalParams = new HashMap<>();
        when(taskExecutionContext.getPrepareParamsMap()).thenReturn(globalParams);

        // Execute the handle method
        switchLogicTask.handle();

        // Verify the task status
        assertEquals(TaskExecutionStatus.SUCCESS, taskExecutionContext.getCurrentExecutionStatus());
    }

    @Test
    void testHandle_DefaultBranch_WhenNoConditionMatches() throws MasterTaskExecuteException {
        // Set up switch parameters with no matching branch condition
        List<SwitchResultVo> switchResultVoList = new ArrayList<>();
        SwitchResultVo switchResultVo = new SwitchResultVo();
        switchResultVo.setCondition("false"); // condition that doesn't match
        switchResultVo.setNextNode(123L); // mock next branch node
        switchResultVoList.add(switchResultVo);

        switchParameters.setSwitchResult(new SwitchParameters.SwitchResult(switchResultVoList, 999L));

        // Execute the handle method
        switchLogicTask.handle();

        // Verify the task params and status
        verify(taskExecutionContext).setCurrentExecutionStatus(TaskExecutionStatus.SUCCESS);
        assertEquals(999L, switchParameters.getNextBranch()); // Default branch should be selected
    }

    @Test
    void testHandle_ExceptionThrownWhenBranchDoesNotExist() {
        // Mock the scenario where the branch does not exist in the workflow graph
        when(workflowExecutionRunnable.getWorkflowExecuteContext()
                .getWorkflowGraph()
                .getTaskNodeByCode(anyLong())).thenReturn(null);

        // Execute the handle method and expect an exception
        MasterTaskExecuteException thrown = assertThrows(MasterTaskExecuteException.class, () -> {
            switchLogicTask.handle();
        });

        // Verify that the exception message is as expected
        assertTrue(thrown.getMessage().contains("please check the switch task configuration"));
    }

    @Test
    void testHandle_ThrowsExceptionWhenDefaultBranchIsMissing() {
        // Set up switch parameters without a default branch
        switchParameters.setSwitchResult(new SwitchParameters.SwitchResult(new ArrayList<>(), null));

        // Execute the handle method and expect an IllegalArgumentException
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> {
            switchLogicTask.handle();
        });

        // Verify that the exception message is as expected
        assertTrue(thrown.getMessage().contains("please check the switch task configuration"));
    }

}
