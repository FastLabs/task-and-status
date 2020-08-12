package task

import org.junit.Assert.*
import org.junit.Test


class HierarchyMatchTest {
    /**
     * No hierarchy root available
     */
    @Test fun hierarchyMatchTest1() {
        val t0 = TaskSpec(id = "t0")
        val speckMatch = TaskSpecMatch(root = t0)
        val hierarchyMatch = HierarchyMatch(specMatch = speckMatch)
        val newMatch = hierarchyMatch.fill(mapOf(), setOf("E1"))
        assertNotNull(newMatch.hierarchyRoot)
        assertEquals(t0, newMatch.hierarchyRoot?.taskSpec)
        assertEquals(0, newMatch.hierarchyRoot?.subTasks?.size)
    }

    @Test fun hierarchyMatchTest2() {
        val t1 = TaskSpec(id = "t1", preConditions = listOf(DependencyDefinition("E1")))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t1))
        val specMatch = TaskSpecMatch(root = t0, matched = setOf(t1))
        val hierarchyMatch = HierarchyMatch(specMatch = specMatch, hierarchyRoot = t0.newTaskInstance(mapOf()))
        val newMatch = hierarchyMatch.fill(mapOf(), setOf("E1"))
        assertEquals(t0, newMatch.hierarchyRoot?.taskSpec)
        assertEquals(1, newMatch.hierarchyRoot?.subTasks?.size)
        val (t1Inst) = newMatch.hierarchyRoot?.subTasks!!
        assertEquals(t1, t1Inst.taskSpec)
        assertTrue ( t1Inst.dependencyMeet("E1") )

    }

}