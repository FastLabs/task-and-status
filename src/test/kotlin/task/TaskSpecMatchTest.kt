package task

import org.junit.Assert.assertEquals
import org.junit.Ignore
import org.junit.Test

class TaskSpecMatchTest {
    /**
     *  a speck match with a missing dependency in it,
     *  it should not be the case as when we match against the task speck repo the match will have the root element
     *  only when has a dependency,
     *  this test is to identify the behaviour: null pointer exception
     *
     *
     *  NOTE: not true anymore, ignored. It is possible to create a hierarchy without dependency in it, it would just not satisfy the dependency
     *  TODO: rewrite the test
     */

    @Ignore @Test(expected = NullPointerException::class) fun testNoDependsOnInList() {
        val t0 = TaskSpec(id = "T0")
        val t0SpecMatch = TaskSpecMatch(root = t0)
        t0SpecMatch.newHierarchy(attributes = mapOf(), matchDependencies = setOf("E0"))
    }

    /**
     * Single task hierarchy
     */
    @Test fun testTaskHierarchy1() {
        val taskSpec = TaskSpec(id = "T2",
                preConditions = listOf(DependencyDefinition("E0")),
                attributes = mapOf(attrDef("cobDate")))
        val specMatch = TaskSpecMatch(root = taskSpec)
        val hierarchyInst = specMatch.newHierarchy(mapOf("cobDate" to "20160101"), setOf("E0"))
        assertEquals("T2", hierarchyInst.taskSpec.id)
        assertEquals(0, hierarchyInst.subTasks.size)
        assertEquals("20160101", hierarchyInst.attributes[0].value)

    }

    /**
     * Two level hierarchy
     */
    @Test fun testTaskHierarchy2() {

        val t1Spec = TaskSpec(id = "T1",
                preConditions = listOf(DependencyDefinition("E1")),
                attributes = mapOf(attrDef("cobDate")))
        val t0Spec = TaskSpec(id = "T0",
                subTasks = setOf(t1Spec),
                attributes = mapOf(attrDef("cobDate")))

        val specMatch = TaskSpecMatch(root = t0Spec, matched = setOf(t1Spec))
        val hierarchy = specMatch.newHierarchy(matchDependencies = setOf("E1"), attributes = mapOf("cobDate" to "20160101"))
        assertEquals("T0", hierarchy.taskSpec.id)
        assertEquals(1, hierarchy.attributes.size)
        assertEquals("20160101", hierarchy.attributes[0].value)

        assertEquals(1, hierarchy.subTasks.size)

        val (t1Inst) = hierarchy.subTasks
        assertEquals("T1", t1Inst.taskSpec.id)
        assertEquals(1, t1Inst.attributes.size)
        assertEquals("20160101", t1Inst.attributes[0].value)
    }

    /**
     * three level hierarchy
     * \T0
     *  -\T1
     *    -\T2
     */
    @Test fun testTaskHierarchy3() {
        val commonAttributes = mapOf(attrDef("cobDate", true))

        val t2Spec = TaskSpec(id = "T2",
                preConditions = listOf(DependencyDefinition("E2")),
                attributes = commonAttributes)
        val t1Spec = TaskSpec(id = "T1",
                subTasks = setOf(t2Spec),
                attributes = commonAttributes)

        val t0Spec = TaskSpec(id = "T0",
                subTasks = setOf(t1Spec),
                attributes = commonAttributes)

        val specMatch = TaskSpecMatch(root = t0Spec, matched = setOf(t2Spec))
        val hierarchy = specMatch.newHierarchy(matchDependencies = setOf("E2"), attributes = mapOf("cobDate" to "20160101"))
        assertEquals("T0", hierarchy.taskSpec.id)
        assertEquals("T0-20160101", hierarchy.id)
        assertEquals(1, hierarchy.attributes.size)
        assertEquals("20160101", hierarchy.attributes[0].value)

        assertEquals(1, hierarchy.subTasks.size)
        val (t1Inst) = hierarchy.subTasks
        assertEquals("T1", t1Inst.taskSpec.id)
        assertEquals("T1-20160101", t1Inst.id)
        assertEquals(1, t1Inst.subTasks.size)
        val (t2Inst) = t1Inst.subTasks
        assertEquals("T2", t2Inst.taskSpec.id)
        assertEquals("T2-20160101", t2Inst.id)

    }

    /**
     * Simple test when no matches specks, empty list should be returned
     * TODO: validate if this is a valid scenario, and check if the event to task matcher populates as expected when a single node task speck is matched
     */
    @Test fun testSinglePath() {
        val t0 = TaskSpec(id = "t0")
        val specMatch = TaskSpecMatch(root = t0)
        val paths = specMatch.paths()

        assertEquals(0, paths.size)
    }

    /**
     * Test double matches in a three level hierarchy
     */
    @Test fun testPaths3LevelHierarchy() {
        val t21 = TaskSpec(id = "t21")
        val t22 = TaskSpec(id = "t22")
        val t1 = TaskSpec(id = "t1", subTasks = setOf(t21, t22))
        val t0 = TaskSpec(id = "t1", subTasks = setOf(t1))
        val specMatch = TaskSpecMatch(root = t0, matched = setOf(t21, t22))
        val paths = specMatch.paths()
        assertEquals(2, paths.size)
        val (path1, path2) = paths
        assertEquals(3, path1.size)
        assertEquals(3, path2.size)
        val (p1L0, p1L1, p1L2) = path1
        assertEquals(p1L0, t0)
        assertEquals(p1L1, t1)
        assertEquals(p1L2, t21)
    }

}