package task


import orchestration.RouteTaskAction
import org.junit.Assert.*
import org.junit.Test
import task.TaskStatus.COMPLETED

class TasksTest {

    @Test fun testNewTask() {
        val taskDefinition = TaskSpec(id = "T1")
        val t1Inst = taskDefinition.newTaskInstance()
        assertNotNull(t1Inst)
        assertEquals("T1", t1Inst.taskSpec.id)
        assertEquals("T1", t1Inst.id)
    }

    /**
     * Test the task creation wit id generation when id contains the task attributes
     */
    @Test fun testNewTaskWithArgsIdGen() {
        val taskDefinition = TaskSpec(id = "T2", attributes = mapOf(attrDef("ID", true)))
        val tInst = taskDefinition.newTaskInstance(mapOf("ID" to "HELLO"))
        assertEquals("T2-HELLO", tInst.id)
    }

    /**
     * Single task with single pre-condition
     */
    @Test fun testPreconditions() {
        val taskSpec = TaskSpec(id = "T0", preConditions = listOf(DependencyDefinition("E1")))
        val taskInst = taskSpec.newTaskInstance(id = "t0Inst", matchDependencies = setOf("E1"))
        assertEquals(1, taskInst.dependsOn.size)
        val (preCondition1) = taskInst.dependsOn
        assertEquals("E1", preCondition1.name)
        assertEquals(true, preCondition1.completed)
    }

    /**
     * Test the hierarchy each level has it's own precodnition
     */
    @Test fun testPreConditions() {

        val T1 = TaskSpec(id = "T1",
                preConditions = listOf(DependencyDefinition("E1")))
        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1),
                preConditions = listOf(DependencyDefinition("E0")))
        val specMatch = TaskSpecMatch(root = T0, matched = setOf(T1))
        val hierarchy = specMatch.newHierarchy(matchDependencies = setOf("E1"))
        assertEquals(T0, hierarchy.taskSpec)
        assertEquals(1, hierarchy.subTasks.size)
        assertEquals(T1, hierarchy.subTasks[0].taskSpec)
        assertEquals(1, hierarchy.dependsOn.size)
        assertEquals("E0", hierarchy.dependsOn[0].name)
        assertEquals(false, hierarchy.dependsOn[0].completed)
        assertEquals(1, hierarchy.subTasks[0].dependsOn.size)
        assertEquals("E1", hierarchy.subTasks[0].dependsOn[0].name)
        assertEquals(true, hierarchy.subTasks[0].dependsOn[0].completed)

    }

    @Test fun testTaskAttributes() {
        val taskSpec = TaskSpec(id = "T2", attributes = mapOf(attrDef("cobDate")))
        val taskInst = taskSpec.newTaskInstance(id = "t2", attributes = mapOf("cobDate" to "20160101"))
        assertEquals(1, taskInst.attributes.size)
        assertEquals("20160101", taskInst.attributes[0].value)
    }



    @Test fun testAllDependenciesMeet() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))
        val task = TaskInstance(id = "${t0.id}-inst",
                taskSpec = t0,
                dependsOn = listOf(TaskDependency(name = "EV_PARTY", completed = true)))
        assertTrue(task.allDependenciesMeet())
    }

    @Test fun testAllDependenciesNotMeet() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))
        val task = TaskInstance(
                id = "${t0.id}-inst",
                taskSpec = t0, dependsOn = listOf(TaskDependency(name = "EV_ACCOUNT", completed = true),
                TaskDependency(name = "EV_ACCOUNT_BAL", completed = false)))
        assertFalse(task.allDependenciesMeet())
    }

    private val cobDateAttribute = TaskAttribute(definition = TaskAttributeDefinition("cobDate", true), value = "20160101")
    private val ukRegionAttribute = TaskAttribute(definition = TaskAttributeDefinition("region", false), value = "uk")

    //TODO: move this out
    /**
     * Task Specification:
     *  |-T0
     *   |-T1
     *  T0 instance exists and we want to attach T1 instance to T0
     *
     */
    @Test fun fillTaskPath() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))
        val t0Inst = t0.newTaskInstance(id = "t0-inst", attributes = mapOf("cobDate" to "20160101"))
        val newT0Inst = t0Inst.add(attributes = mapOf("region" to "uk"), path = listOf(t0, t1))
        assertEquals(t0, newT0Inst.taskSpec)
        assertEquals(1, newT0Inst.subTasks.size)
        val (t1Inst) = newT0Inst.subTasks
        assertEquals(t1Inst.taskSpec, t1)
        assertEquals(TaskStatus.PENDING, t1Inst.status)
        assertEquals(0, t1Inst.subTasks.size)
        //assertEquals(listOf(cobDateAttribute, ukRegionAttribute), t1Inst.attributes) //TODO: fix the attribute from parent
    }

    /**
     * Task Specification. T0 has 2 children tasks at the same level
     *  |-T0
     *   |-T1
     *   |-T2
     * T0 and T1 instance exists when adding T2 will be appended to T0
     */
    @Test fun fillTaskPath1() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t2 = TaskSpec(id = "T2",
                taskAction = RouteTaskAction("ETL_SERVICE"))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1, t2),
                attributes = mapOf(attrDef("cobDate", true)))

        val t0Inst = t0.newTaskInstance(id = "t0-inst", attributes = mapOf("cobDate" to "20160101"))
        val newT0Inst1 = t0Inst.add(mapOf("region" to "uk"), listOf(t0, t1))
        assertEquals(t0, newT0Inst1.taskSpec)
        assertEquals(1, newT0Inst1.subTasks.size)
        val newT0Inst2 = newT0Inst1.add(mapOf("cobDate" to "20160101"), listOf(t0, t2))
        assertEquals(t0, newT0Inst2.taskSpec)
        assertEquals(2, newT0Inst2.subTasks.size)
        val (t0Sub1, t0Sub2) = newT0Inst2.subTasks
        assertEquals(t1, t0Sub1.taskSpec)
        assertEquals(t2, t0Sub2.taskSpec)
    }

    /**
     * Task Hierarchy Specification
     *  |-T0
     *   |-T1
     *     |-T11
     * T0 instance is created when adding T11, T1 should be created
     */
    @Test fun fillTaskPath2() {
        val t11 = TaskSpec(id = "T11")
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                subTasks = setOf(t11),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))

        val t0Inst = t0.newTaskInstance(id = "t0-inst", attributes = mapOf("cobDate" to "20160101"))
        val newT0Inst1 = t0Inst.add(attributes = mapOf("region" to "uk"), path = listOf(t0, t1, t11))
        assertEquals(1, newT0Inst1.subTasks.size)
        val (t1Inst) = newT0Inst1.subTasks
        assertEquals(t1, t1Inst.taskSpec)
        assertEquals(1, newT0Inst1.subTasks[0].subTasks.size)
        val (t11Inst) = t1Inst.subTasks
        assertEquals(t11, t11Inst.taskSpec)
        assertEquals(0, t11Inst.subTasks.size)
    }

    /**
     * Task Hierarchy Specification
     *  |-T0
     *    |-T1
     *      |-T11
     *    |-T2
     *      |-T21
     * Partial Task Instance hierarchy exists
     *  |-T0i
     *    |-T1i
     *      |-T11i
     * Once trying to fill the path T0 T2 T21 the following hierarchy should be created
     *  |-T0i
     *    |-T1i
     *      |-T11i
     *    |-T2i
     *      |-T21i
     */
    @Test fun fillTaskPath3() {
        val t11 = TaskSpec(id = "T11", attributes = mapOf(attrDef("region"), attrDef("cobDate", true)))
        val t21 = TaskSpec(id = "T21", attributes = mapOf(attrDef("cobDate", true)))

        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                subTasks = setOf(t11)
        )

        val t2 = TaskSpec(id = "T2",
                subTasks = setOf(t21),
                taskAction = RouteTaskAction("ETL_SERVICE")
        )
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1, t2),
                attributes = mapOf(attrDef("cobDate", true)))
        val t0Inst = t0.newTaskInstance(id = "T0-inst", attributes = mapOf("cobDate" to "20160101"))
        val t0Inst1 = t0Inst.add(mapOf("cobDate" to "20160101", "region" to "uk"), listOf(t0, t1, t11))
        val t0Inst2 = t0Inst1.add(mapOf("cobDate" to "20160101"), listOf(t0, t2, t21))
        assertEquals(2, t0Inst2.subTasks.size)
        val (t1Inst, t2Inst) = t0Inst2.subTasks
        assertEquals(t1, t1Inst.taskSpec)
        assertEquals(1, t1Inst.subTasks.size)
        assertEquals(TaskStatus.PENDING, t0Inst.status)
        assertEquals(t2, t2Inst.taskSpec)
        assertEquals(1, t2Inst.subTasks.size)

        assertEquals(1, t1Inst.subTasks.size)
        val (t11Inst) = t1Inst.subTasks
        assertEquals(t11, t11Inst.taskSpec)

        assertEquals(listOf(ukRegionAttribute, cobDateAttribute), t11Inst.attributes)

        assertEquals(1, t2Inst.subTasks.size)
        val (t21Inst) = t2Inst.subTasks
        assertEquals(t21, t21Inst.taskSpec)

    }

    /**
     * A new task is required to be added on a existing path. The task once in COMPLETED status should not be changed and
     * a conflict resolution (unroutable) action should be triggered for the
     */

    @Test fun fillTaskPath4() {
        val defaultAttributes = mapOf("cobDate" to "20160101")

        val t01 = TaskSpec(id = "T01",

                attributes = mapOf(attrDef("cobDate")))
        val t00 = TaskSpec(id = "T00",
                subTasks = setOf(t01),
                attributes = mapOf(attrDef("cobDate")))
        val speckMatch = TaskSpecMatch(root = t00, matched = setOf(t01))
        val t00Inst = speckMatch.newHierarchy(defaultAttributes, setOf())

        val t00Hierarchy1 = t00Inst.add(defaultAttributes, listOf(t00, t01))
        val completedT01 = t00Hierarchy1.subTasks[0].copy(status = COMPLETED)
        val t00Hierarchy2 = t00Hierarchy1.copy(subTasks = listOf(completedT01))
        val t00Hierarchy3 = t00Hierarchy2.add(defaultAttributes, listOf(t00, t01))
        println(t00Hierarchy3.subTasks[0])
        fail("Finish this test")

    }

    /**
     * Tests if a hierarchy instance contains a particular task-id
     */
    @Test fun testContain() {

        val t2 = TaskSpec(id = "T2")
        val t1 = TaskSpec(id = "T1", subTasks = setOf(t2))
        val t1Inst = t1.newTaskInstance("t1-inst")
                .copy(subTasks = listOf(t2.newTaskInstance("t2-inst")))
        assertTrue(t1Inst.containTask("t2-inst"))
    }

    @Test fun testSelectArguments() {
        val t1 = TaskSpec(id = "T", attributes = mapOf(attrDef("cobDate", true)))
        val arguments = t1.selectTaskArguments(mapOf("cobDate" to "20160101", "region" to "uk")).toList()
        assertEquals(1, arguments.size)
        val (arg1) = arguments
        assertEquals("cobDate", arg1.definition.name)
        assertTrue(arg1.definition.taskArgument)
    }

    @Test fun testMatchAttributesTask() {
        val t1 = TaskSpec(id = "T1", attributes = mapOf(attrDef("cobDate", true)))
        val t1Inst = t1.newTaskInstance(mapOf("cobDate" to "20160101"))
        val taskAttributes1 = listOf(TaskAttribute(definition = TaskAttributeDefinition("cobDate", true), value = "20160101"))
        assertTrue(t1Inst.matchAttributes(taskAttributes1))

        val taskAttributes2 = listOf(TaskAttribute(definition = TaskAttributeDefinition("cobDate", true), value = "20160102"))
        assertFalse(t1Inst.matchAttributes(taskAttributes2))
    }

    @Test fun testFindTaskInHierarchy() {

        val t6 = TaskSpec(id = "T6",
                preConditions = listOf(DependencyDefinition("E6")),
                attributes = mapOf(attrDef("cobDate", true)))
        val t5 = TaskSpec(id = "T5", subTasks = setOf(t6))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t5),
                attributes = mapOf(attrDef("cobDate", true)))
        val specMatch = TaskSpecMatch(root = t0, matched = setOf(t6))
        val hierarchy = specMatch.newHierarchy(attributes = mapOf("cobDate" to "20160101"), matchDependencies = setOf("E6"))
        val task = hierarchy.findSubBySpecId("T6")
        assertEquals("T6-20160101", task?.id)
    }

    @Test fun findTaskInSingleHierarchy() {
        val t0 = TaskSpec(id = "T0")
        val specMatch = TaskSpecMatch(root = t0, matched = setOf(t0))
        val hierarchy = specMatch.newHierarchy()
        val x = hierarchy.findSubBySpecId("T0")
        assertEquals(t0, x?.taskSpec)
    }


}
