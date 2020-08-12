package orchestration.rules


import orchestration.NoTaskAction
import orchestration.PersistTaskAction
import orchestration.RouteTaskAction
import org.junit.Assert.*
import org.junit.Test
import task.*
import task.TaskStatus.*



class TaskRulesTest {

    private val ETL_SERVICE = RouteTaskAction("ETL_SERVICE")
    /**
     * A single task with a no action attached no dependencies and no sub-tasks will be marked as complete
     */
    @Test fun testPendingSchedule1() {
        val t0 = TaskSpec(id = "T0",
                description = "SINGLE TASK",
                attributes = mapOf(attrDef("cobDate", true)))
        val t = TaskInstance(id = "task1", taskSpec = t0)  //PENDING
        val actions = t.evaluateActions()
        assertEquals(1, actions.size)
        val (actionType, task) = actions[0]

        assertTrue ( actionType is PersistTaskAction )
        assertEquals(COMPLETED, task.status)
        assertNotNull(task.endTime)
        assertNotNull(task.startTime)
        assertEquals(task.startTime, task.endTime)
    }

    /**
     * A single task with ETL action no dependencies and no sub-tasks the task will be scheduled
     */
    @Test fun testPendingSchedule2() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))

        val t = TaskInstance(id = "task2", taskSpec = t1) //PENDING
        val actions = t.evaluateActions()
        assertEquals(1, actions.size)
        val (actionType, task) = actions[0]
        assertEquals(ETL_SERVICE, actionType)
        assertEquals(TaskStatus.SCHEDULED, task.status)
        assertNotNull(task.scheduleTime)
    }

    /**
     * Parent task with no action. Sub-Task completed, as result root task will be completed as well
     */
    @Test fun testPendingSchedule3() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))

        val sub = TaskInstance(id = "task1", taskSpec = t1, status = COMPLETED)
        val parent = TaskInstance(id = "task2", taskSpec = t0, subTasks = listOf(sub))
        val actions = parent.evaluateActions()
        assertEquals(1, actions.size)
        val (actionType, task) = actions [0]
        assertTrue ( actionType is PersistTaskAction )
        assertEquals(COMPLETED, task.status)
    }

    /**
     * A parent Task with single children, both in PENDING status. Parent will stay in the same status the sub-task
     * will be scheduled
     * IN:
     *  \T0   - PENDING
     *  \- T1 - PENDING -> ETL_SERVICE
     *
     * ESTIMATED TASK STATE:
     *  \TO   - PENDING
     *   \-T1 - SCHEDULED
     */
    @Test fun testPendingSchedule4() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))
        val sub = TaskInstance(id = "T1", taskSpec = t1)                                //in PENDING STATUS
        val parent = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(sub))    //in PENDING status
        val actions = parent.evaluateActions()
        assertEquals(1, actions.size)
        val (actionType, task) = actions[0]

        assertEquals(ETL_SERVICE, actionType)
        assertEquals(t1, task.taskSpec)
        assertEquals(TaskStatus.SCHEDULED, task.status)
        assertNotNull(task.scheduleTime)
    }

    /**
     * Three level task hierarchy. First 2 levels are in PENDING and the leaf task is in COMPLETED state. As result
     * both parent tasks should be COMPLETED
     * IN:
     *  \TO      - PENDING
     *   \- T5    - PENDING
     *    \- T6   - COMPLETED
     * OUT:
     *
     * ESTIMATED TASk STATE:
     *  \TO      - COMPLETED
     *   \- T5    - COMPLETED
     *     \- T6   - COMPLETED
     */
    @Test fun testPendingSchedule5() {

        val t6 = TaskSpec(id = "t6")
        val t5 = TaskSpec(id = "t5", subTasks = setOf(t6))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t5))

        val sub = TaskInstance(id = "T6", taskSpec = t6, status = COMPLETED)
        val sub1 = TaskInstance(id = "T5", taskSpec = t5, subTasks = listOf(sub))                     //in PENDING status
        val parent = TaskInstance(id = "TO", taskSpec = t0, subTasks = listOf(sub1))

        val actions = parent.evaluateActions()
        assertEquals(2, actions.size)
        val (action1, action2) = actions
        assertTrue ( action1.action is PersistTaskAction )
        assertEquals(t0, action1.task.taskSpec)
        assertEquals(TaskStatus.COMPLETED, action1.task.status)

        assertTrue ( action2.action is PersistTaskAction )
        assertEquals(t5, action2.task.taskSpec)
        assertEquals(TaskStatus.COMPLETED, action2.task.status)

    }

    /**
     * Three level task hierarchy. First 2 levels are in PENDING and the leaf task is in FAILED state. No action specified for parents
     * As result both parent tasks should be FAILED
     * IN:
     *  \T0    - PENDING
     *  \- T5  - PENDING
     *  \-- T6 - FAILED
     *
     * ESTIMATED TASK STATE:
     *  \T0    - FAILED
     *  \- T5  - FAILED
     *  \-- T6 - FAILED
     */
    @Test fun pendingSchedule6() {
        val t6 = TaskSpec(id = "t6")
        val t5 = TaskSpec(id = "t5", subTasks = setOf(t6))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t5))
        val sub = TaskInstance(id = "T6", taskSpec = t6, status = FAILED)
        val sub1 = TaskInstance(id = "T5", taskSpec = t5, subTasks = listOf(sub))
        val parent = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(sub1))
        val actions = parent.evaluateActions()
        assertEquals(2, actions.size)
        val (action1, action2) = actions
        assertTrue ( action1.action is PersistTaskAction )
        assertEquals(t0, action1.task.taskSpec)
        assertEquals(FAILED, action1.task.status)
        assertTrue ( action2.action is PersistTaskAction )
        assertEquals(t5, action2.task.taskSpec)
        assertEquals(FAILED, action2.task.status)

    }

    /**
     * Three level hierarchy. First 2 levels are in PENDING and leaf task is in COMPLETED state. No action specified for the parents.
     * As result bot parents should be in COMPLETED state
     * IN:
     *  \T0    - PENDING
     *   \- T5  - PENDING
     *     \- T6 - COMPLETED
     *
     * ESTIMATED TASK STATE:
     *  \T0    - COMPLETED
     *   \- T5  - COMPLETED
     *     \- T6 - COMPLETED
     *
     */
    @Test fun pendingSchedule7() {
        val t6 = TaskSpec(id = "t6")
        val t5 = TaskSpec(id = "t5", subTasks = setOf(t6))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t5))
        val sub = TaskInstance(id = "T6", taskSpec = t6, status = COMPLETED)
        val sub1 = TaskInstance(id = "T5", taskSpec = t5, status = PENDING, subTasks = listOf(sub))
        val parent = TaskInstance(id = "T0", taskSpec = t0, status = PENDING, subTasks = listOf(sub1))
        val actions = parent.evaluateActions()

        assertEquals(2, actions.size)
        val (action1, action2) = actions

        assertTrue ( action1.action is PersistTaskAction )
        assertEquals(t0, action1.task.taskSpec)

        assertTrue ( action2.action is PersistTaskAction )

        assertEquals(t5, action2.task.taskSpec)
    }

    /**
     * An extension test for pendingSchedule7 which has 2 leaf sub-tasks COMPLETED. The parent hierarchy will COMPLETE as well
     * IN:
     *  \T0     - PENDING
     *  \- T5   - PENDING
     *  \-- T6  - COMPLETED
     *  \-- T61 - COMPLETED
     *
     * ESTIMATED TASK STATE:
     *  \T0     - COMPLETED
     *  \- T5   - COMPLETED
     *  \-- T6  - COMPLETED
     *  \-- T61 - COMPLETED
     */
    @Test fun pendingSchedule9() {
        val t61 = TaskSpec(id = "t61")
        val t6 = TaskSpec(id = "t6")
        val t5 = TaskSpec(id = "t5", subTasks = setOf(t6, t61))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t5))

        val it61 = TaskInstance(id = "T61", taskSpec = t61, status = COMPLETED)
        val it6 = TaskInstance(id = "T6", taskSpec = t6, status = COMPLETED)
        val it5 = TaskInstance(id = "T5", taskSpec = t5, subTasks = listOf(it6, it61))
        val it0 = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(it5))
        val actions = it0.evaluateActions()
        assertEquals(2, actions.size)
        val (action1, action2) = actions
        assertTrue ( action1.action is PersistTaskAction )
        assertEquals(TaskStatus.COMPLETED, action1.task.status)
        assertEquals(t0, action1.task.taskSpec)
        assertTrue ( action2.action is PersistTaskAction )
        assertEquals(TaskStatus.COMPLETED, action2.task.status)
        assertEquals(t5, action2.task.taskSpec)
    }


    /**
     * An extension test for pendingSchedule6 which has 2 leaf sub-tasks one COMPLETED and another FAILED
     * The parent hierarchy will FAILED as well
     * IN:
     *  \T0     - PENDING
     *  \- T5   - PENDING
     *  \-- T6  - FAILED
     *  \-- T61 - COMPLETED
     *
     * ESTIMATED TASK STATE:
     *  \T0     - FAILED
     *  \- T5   - FAILED
     *  \-- T6  - FAILED
     *  \-- T61 - COMPLETED
     */
    @Test fun pendingSchedule10() {
        val t61 = TaskSpec(id = "t61")
        val t6 = TaskSpec(id = "t6")
        val t5 = TaskSpec(id = "t5", subTasks = setOf(t6, t61))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t5))
        val it61 = TaskInstance(id = "T61", taskSpec = t61, status = COMPLETED)
        val it6 = TaskInstance(id = "T6", taskSpec = t6, status = FAILED)
        val it5 = TaskInstance(id = "T5", taskSpec = t5, subTasks = listOf(it6, it61))
        val it0 = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(it5))
        val actions = it0.evaluateActions()
        assertEquals(2, actions.size)
        val (action1, action2) = actions
        assertTrue ( action1.action is PersistTaskAction )
        assertEquals(TaskStatus.FAILED, action1.task.status)
        assertEquals(t0, action1.task.taskSpec)
        assertTrue ( action2.action is PersistTaskAction )
        assertEquals(TaskStatus.FAILED, action2.task.status)
        assertEquals(t5, action2.task.taskSpec)
    }


    /**
     * Three level hierarchy. First 2 levels are in PENDING, and leaf task is COMPLETED. An Action is specified for the
     * middle task. The middle task should be in SCHEDULED with Specified action to be triggered , the root task still stays
     * in PENDING
     * IN:
     *  \T0    - PENDING
     *  \- T7  - PENDING => ETL_SERVICE action
     *  \-- T8 - COMPLETED
     *
     * ESTIMATED TASK STATE:
     *  \T0    - PENDING
     *  \- T7  - SCHEDULED => ETL_SERVICE
     *  \-- T8 - COMPLETED
     *
     */
    @Test fun pendingSchedule8() {
        val t8 = TaskSpec(id = "t8", preConditions = listOf(DependencyDefinition("E7")), taskAction = ETL_SERVICE)
        val t7 = TaskSpec(id = "t7", subTasks = setOf(t8), taskAction = ETL_SERVICE)
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t7))
        val sub = TaskInstance(id = "T8", taskSpec = t8, status = COMPLETED)
        val sub1 = TaskInstance(id = "T7", taskSpec = t7, subTasks = listOf(sub))
        val parent = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(sub1))
        assertTrue ( parent.isHierarchyComplete() )
        val actions = parent.evaluateActions()
        assertEquals(1, actions.size)
        val (action1) = actions
        assertEquals(ETL_SERVICE, action1.action)
        assertEquals(t7, action1.task.taskSpec)
        assertEquals(TaskStatus.SCHEDULED, action1.task.status)
    }


    /**
     * Test if only one action will be triggered from a list of sub-tasks. First task from the list will be executed
     * IN:
     *  \T0    - PENDING
     *  \- T7   - PENDING => ETL_SERVICE action
     *  \- T71  - PENDING => ETL SERVICE action depends on T7 not yet satisfied
     *
     * ESTIMATED TASK STATE
     *  \T0     - PENDING
     *  \- T7   - SCHEDULED
     *  \- T71  - PENDING => ETL SERVICE action
     *
     */
    @Test fun pendingSchedule11() {
        val t71 = TaskSpec(id = "T71",
                preConditions = listOf(DependencyDefinition(name = "T7")),
                taskAction = ETL_SERVICE,
                attributes = mapOf(attrDef("cobDate", true)))

        val t7 = TaskSpec(id = "T7",
                taskAction = ETL_SERVICE)

        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t7, t71),
                attributes = mapOf(attrDef("cobDate", true)))

        val it71 = TaskInstance(id = "T71", taskSpec = t71, dependsOn = listOf(TaskDependency(name = "T7", completed = false)))
        val it7 = TaskInstance(id = "T7", taskSpec = t7)
        val it0 = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(it7, it71))
        assertTrue ( it0.isHierarchyComplete() )
        val actions = it0.evaluateActions()
        assertEquals(1, actions.size)
        val (action1) = actions
        assertEquals(ETL_SERVICE, action1.action)
        assertEquals(t7, action1.task.taskSpec)
        assertEquals(TaskStatus.SCHEDULED, action1.task.status)
    }

    /**
     * Two level hierarchy there is no dependencies between sub-tasks. This means that that tasks can be executed in parallel
     *
     * IN:
     *  \T0    - PENDING
     *  \- T7   - PENDING => ETL_SERVICE action
     *  \- T71  - PENDING => ETL SERVICE action
     *
     * ESTIMATED TASK STATE
     *  \T0     - PENDING
     *  \- T7   - SCHEDULED => ETL_SERVICE action
     *  \- T71  - SCHEDULED => ETL_SERVICE action
     *
     */

    @Test fun pendingSchedule12() {
        val t71 = TaskSpec(id = "T71",
                taskAction = ETL_SERVICE,
                attributes = mapOf(attrDef("cobDate", true))
        )
        val t7 = TaskSpec(id = "T7",
                taskAction = ETL_SERVICE)

        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t7, t71),
                attributes = mapOf(attrDef("cobDate", true)))

        val it71 = TaskInstance(id = "T71", taskSpec = t71)
        val it7 = TaskInstance(id = "T7", taskSpec = t7)

        val it0 = TaskInstance(id = "T0", taskSpec = t0, subTasks = listOf(it7, it71))

        val actions = it0.evaluateActions()
        assertEquals(2, actions.size)
        val (action1, action2) = actions
        assertEquals(ETL_SERVICE, action1.action)
        assertEquals(t7, action1.task.taskSpec)
        assertEquals(TaskStatus.SCHEDULED, action1.task.status)
        assertEquals(ETL_SERVICE, action2.action)
        assertEquals(t71, action2.task.taskSpec)
        assertEquals(TaskStatus.SCHEDULED, action2.task.status)
    }


    /**
     * Two level hierarchy. If rules are invoked when a partial hierarchy is filled and the elements of the
     * hierarchy is in COMPLETE status. The root node should stay in pending until the hierarchy is filled
     * Hierarchy spec:
     *  \T0
     *    -\T1
     *    -\T2
     *
     * Task Instances - before running the rules
     *  \t0 - PENDING
     *    -\t1 - COMPLETED
     *
     */

    @Test fun testIncompleteHierarchy() {
        val t1 = TaskSpec(id = "T1", preConditions = listOf(DependencyDefinition("E1")))
        val t2 = TaskSpec(id = "T2", preConditions = listOf(DependencyDefinition("E2")))
        val t0 = TaskSpec(id = "T0", subTasks = setOf(t1, t2))
        val hierarchyMatch = TaskSpecMatch(root = t0, matched = setOf(t1))
        val t0Hierarchy = hierarchyMatch.newHierarchy(matchDependencies = setOf("E1"))
        assertEquals(1, t0Hierarchy.subTasks.size)
        assertFalse ( t0Hierarchy.isHierarchyComplete() )
        val (t1Instance) = t0Hierarchy.subTasks
        assertEquals(PENDING, t1Instance.status)
        val t0Updated = t0Hierarchy.updateSub(t1Instance.copy(status = COMPLETED))

        val (action, evaluatedHierarchy) = t0Updated.evaluateActions()[0]
        assertTrue ( action is NoTaskAction )
        assertEquals(PENDING, evaluatedHierarchy.status)

    }

}