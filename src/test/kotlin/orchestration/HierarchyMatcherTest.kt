package orchestration

import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test
import repository.InMemoryTaskInstanceRepo
import repository.TaskRepository
import task.*
import task.TaskStatus.COMPLETED


@Suppress("UNUSED_VARIABLE")
class HierarchyMatcherTest {
    val taskInstanceRepo = InMemoryTaskInstanceRepo()

    private val commonAttributes = mapOf(attrDef("cobDate", true))

    private fun getOrchestrationEvent(eventType: String): OrchestrationEvent {
        val json = JsonObject()
        json.put("cobDate", "20160101")
        return OrchestrationEvent(eventId = "ev-1", eventType = eventType, payload = json)
    }

    /**
     * No Task Instance found for the event, T0 is a simple task without hierarchy
     * a new Task is created with status PENDING
     */
    @Test fun testInstanceMatch1() {
        val T0 = TaskSpec(id = "T0",
                preConditions = listOf(DependencyDefinition("T0")),
                attributes = commonAttributes)
        val taskRepo = TaskRepository(tInstanceRepo = taskInstanceRepo)
        taskRepo.saveSpecs(listOf(T0)).toBlocking().value()

        val taskActions = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T0")).toBlocking().value()
        assertEquals(1, taskActions.size)
        val (action1) = taskActions
        val (taskAction, t0Inst) = action1
        assertEquals(TaskStatus.PENDING, t0Inst.status)
        assertEquals(1, t0Inst.attributes.size)
        assertEquals("T0", t0Inst.taskSpec.id)
        assertEquals(1, t0Inst.dependsOn.size)
        assertEquals("T0", t0Inst.dependsOn[0].name)
        assertEquals(true, t0Inst.dependsOn[0].completed)
    }

    /**
     * Double level hierarchy specification without any instances available
     * Task Spec:
     *  \T0
     *   -\T1 depends on T1 event
     *
     * Instances: none
     *
     * as result a new hierarchy will be created
     */
    @Test fun testInstanceMatch2() {
        val T1 = TaskSpec(id = "T1",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1),
                attributes = commonAttributes)

        val taskRepo = TaskRepository(tInstanceRepo = taskInstanceRepo)
        taskRepo.saveSpecs(listOf(T0))

        val actions = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T1")).toBlocking().value()
        assertEquals(1, actions.size)
        val (action1) = actions
        val (taskAction, t0Inst) = action1
        assertEquals(TaskStatus.PENDING, t0Inst.status)
        assertEquals("T0", t0Inst.taskSpec.id)
        assertEquals(0, t0Inst.dependsOn.size)
        assertEquals(1, t0Inst.subTasks.size)
        val (t1Inst) = t0Inst.subTasks
        assertEquals("T1", t1Inst.taskSpec.id)
        assertEquals(TaskStatus.PENDING, t1Inst.status)
        assertEquals(1, t1Inst.dependsOn.size)
        assertEquals("T1", t1Inst.dependsOn[0].name)
        assertEquals(true, t1Inst.dependsOn[0].completed)
    }

    /**
     * Marches to an existing hierarchy
     * \T0
     *  -\T1 depends on T1 event
     *
     * Available Instances
     * \T0 - PENDING
     *
     * Matched task hierarchy
     * \T0 - PENDING
     *  -\T1 - PENDING, cobDate=20160101
     *
     */
    @Test fun testInstanceMatch3() {
        val T1 = TaskSpec(id = "T1",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1),
                attributes = commonAttributes)
        val t0Inst = T0.newTaskInstance("T0-INST")
        val taskRepo = TaskRepository(tInstanceRepo = taskInstanceRepo)
        taskRepo.saveSpecs(listOf(T0))
        taskRepo.saveInstance(t0Inst).toBlocking().value()

        val actions = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T1")).toBlocking().value()
        assertEquals(1, actions.size)
        val (action1) = actions
        val (taskAction, t0Found) = action1
        assertEquals(T0, t0Found.taskSpec)
        assertEquals(1, t0Found.subTasks.size)
        val (t1Inst) = t0Found.subTasks
        assertEquals(T1, t1Inst.taskSpec)
        assertEquals(TaskStatus.PENDING, t1Inst.status)
        assertTrue ( t1Inst.dependencyMeet("T1") )
    }

    /**
     * Matches to an existing PENDING hierarchy but with the task itself completed, TODO: is not implemented yet
     * but as result the event should be sent to unroutable
     * SPEC
     * \T0
     *  -\T1 depends on T1 event
     *  -\T2 depends on T2 event (not a subject of this test just to represent a valid scenario)
     *
     * Instances (Note: T1 event received and T1 is in COMPLETE status, T0 is int pending waiting for T2)
     * \T0 - PENDING
     *  -\T1 - COMPLETE
     */

    //TODO: this is a valid test but he logic will not be part of the matcher
    @Test fun testInstanceMatch4() {

        val T2 = TaskSpec(id = "T2",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T2")))
        val T1 = TaskSpec(id = "T1",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1, T2),
                attributes = commonAttributes)
        val taskRepo = TaskRepository()

        taskRepo.saveSpecs(listOf(T0))
        val t1Inst = T1.newTaskInstance(id = "T1-INST", attributes = mapOf("cobDate" to "20160101"))
                .copy(status = COMPLETED)
        val t0Inst = T0.newTaskInstance(id = "T0-INST", attributes = mapOf("cobDate" to "20160101"))
                .copy(subTasks = listOf(t1Inst))

        taskRepo.saveInstance(t0Inst).toBlocking().value()
        val matched = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T1")).toBlocking().value()

        println(matched) //TODO: fix the functionality for this scenario
        fail("To implement the feature")

    }

    /**
     * Scenario when there are 2 specifications that depends on the same event, no instances are present, is expected to
     * have a instance for each specification
     */

    @Test fun testInstanceMatch5() {
        val T2 = TaskSpec(id = "T2",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T1 = TaskSpec(id = "T1",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1),
                attributes = commonAttributes)
        val taskRepo = TaskRepository()
        taskRepo.saveSpecs(listOf(T0, T2))

        val matched = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T1")).toBlocking().value()
        assertEquals(2, matched.size)

        val (action1, action2) = matched
        val (ta1, first) = action1
        val (ta2, second) = action2
        assertEquals(T0, first.taskSpec)
        assertEquals(T2, second.taskSpec)
    }


    /**
     * Scenario when one event matches to a task-speck with an existing hierarchy instance and the same event matches a task-speck
     * with no hierarchy
     * Spec:
     * ----------
     * \T0
     *  -\T1
     *   -\T11
     * ----------
     * \T2
     *
     * Available Instances in task instance repository before match:
     * \T0inst - PENDING
     *  -\T1inst - PENDING
     *
     * Matched Instances
     * \T0inst - PENDING
     *  -\T1inst - PENDING
     *
     * \T2 - PENDING
     */

    @Test fun testInstanceMatch6() {
        val T2 = TaskSpec(id = "T2",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T11 = TaskSpec(id = "T11",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T1 = TaskSpec(id = "T1",
                subTasks = setOf(T11))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1),
                attributes = commonAttributes)

        val taskRepo = TaskRepository()
        taskRepo.saveSpecs(listOf(T0, T2))
        val specMatch = TaskSpecMatch(root = T0, matched = setOf(T1))
        val t1Inst = specMatch.newHierarchy()
        taskRepo.saveInstance(t1Inst).toBlocking().value()
        val matched = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T1")).toBlocking().value()
        assertEquals(2, matched.size)
        val (first, second) = matched
        assertEquals("T0-20160101", first.task.id)
        assertEquals(1, first.task.subTasks.size)
        assertEquals("T1", first.task.subTasks[0].taskSpec.id)
        assertEquals("T2-20160101", second.task.id)
    }


    /**
     * Scenario when an event matches two tasks specks under the same hierarchy
     * Task Specification
     * T0
     *  \-T1 - depends on T1 event, args: cobDate
     *  \-T2 - depends on T1 event, args: cobDate
     * Instances
     *
     * a new hierarchy should be created with both paths filled in
     */

    @Test fun testInstanceMatch7() {
        val T2 = TaskSpec(id = "T2",
                attributes = commonAttributes, preConditions = listOf(DependencyDefinition("T1")))
        val T1 = TaskSpec(id = "T1",
                attributes = commonAttributes,
                preConditions = listOf(DependencyDefinition("T1")))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1, T2),
                attributes = commonAttributes)
        val taskRepo = TaskRepository()
        taskRepo.saveSpecs(listOf(T0))
        val matched = taskRepo.matchTaskHierarchy(getOrchestrationEvent("T1")).toBlocking().value()
        assertEquals(1, matched.size)
        val (theOne) = matched
        assertEquals(T0, theOne.task.taskSpec)
        assertEquals(2, theOne.task.subTasks.size)
        val (t1, t2) = theOne.task.subTasks
        assertEquals(T1, t1.taskSpec)
        assertEquals(T2, t2.taskSpec)

    }

}