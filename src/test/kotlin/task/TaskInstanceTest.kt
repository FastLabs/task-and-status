package task

import org.junit.Assert.*
import org.junit.Test
import task.TaskStatus.COMPLETED
import task.TaskStatus.PENDING


class TaskInstanceTest {

    /**
     * Update a three level hierarchy
     * \T0
     *  -\T1
     *    -\T11 - PENDING
     * Calling update with a new instance of a sub-task will return a new hierarchy instance with the updated node
     */
    @Test fun updateSub() {
        val t11 = TaskSpec(id = "t11", preConditions = listOf(DependencyDefinition("E11")))
        val t1 = TaskSpec(id = "t1", subTasks = setOf(t11))
        val t0 = TaskSpec(id ="t1", subTasks = setOf(t1))

        val specMatch = TaskSpecMatch(root = t0, matched = setOf(t11))
        val hierarchy = specMatch.newHierarchy(matchDependencies = setOf("E11"))
        val t11Inst = hierarchy.findSubByTaskId("t11")
        assertEquals("t11", t11Inst?.id)
        assertEquals(PENDING, t11Inst?.status)
        val t11InstUpdated = t11.newTaskInstance().copy(status = COMPLETED)
        val updatedHierarchy = hierarchy.updateSub(t11InstUpdated)
        assertEquals(t0, updatedHierarchy.taskSpec)
        val t11FromHIerarchy = updatedHierarchy.findSubByTaskId("t11")
        assertEquals(t11, t11FromHIerarchy?.taskSpec)
        assertEquals(COMPLETED, t11FromHIerarchy?.status)
    }

    /**
     * Single level hierarchy when try to substitute if the spec is the same will return the substitution, otherwise
     * will return the original instance
     */
    @Test fun updateSub1() {
        val t0 = TaskSpec(id = "T0")
        val t0Inst = t0.newTaskInstance()
        val t0InstModified = t0Inst.copy(status = COMPLETED)
        assertEquals(PENDING, t0Inst.status)
        val newT0 = t0Inst.updateSub(t0InstModified)
        assertEquals(COMPLETED, newT0.status)

        //Scenario 2
        val t1 = TaskSpec(id = "T1")
        val t1Inst = t1.newTaskInstance()
        val t0InstModified1 = t0Inst.updateSub(t1Inst)
        assertEquals(t0Inst, t0InstModified1)
    }

    @Test fun isHierarchyComplete() {
        val t1 = TaskSpec(id="T1", preConditions = listOf(DependencyDefinition("E1")))
        val t0 = TaskSpec(id="T0", subTasks = setOf(t1))
        val t0Inst = t0.newTaskInstance()
        assertFalse ( t0Inst.isHierarchyComplete())

        val match = TaskSpecMatch(root = t0, matched = setOf(t1))
        val hierarchy = match.newHierarchy(matchDependencies = setOf("E1"))
        assertTrue ( hierarchy.isHierarchyComplete() )

    }

    @Test fun testFindSubById() {
        val t1 = TaskSpec(id = "t1", preConditions = listOf(DependencyDefinition("E1")))
        val t2 = TaskSpec(id="t2", preConditions = listOf(DependencyDefinition("E2")))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t1, t2))
        val specMach = TaskSpecMatch(root = t0, matched = setOf(t1, t2))
        val hierarchy = specMach.newHierarchy(matchDependencies = setOf("E1", "E2"))
        assertEquals(hierarchy.taskSpec, t0)
        val (first, second) = hierarchy.subTasks
        assertEquals(t1, first.taskSpec)
        assertEquals(t2, second.taskSpec)
        assertNotNull(hierarchy.findSubBySpecId("t1"))
        assertNotNull(hierarchy.findSubBySpecId("t2"))

    }
}