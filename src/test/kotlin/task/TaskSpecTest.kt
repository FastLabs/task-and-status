package task

import org.junit.Assert.*
import org.junit.Test


class TaskSpecTest {

    @Test fun testAttributes() {
        val def1 = TaskSpec(id = "T1", attributes = mapOf(attrDef("COB_DATE", true)))
        assertEquals(TaskAttributeDefinition("COB_DATE", true), def1["COB_DATE"])
    }

    @Test fun dependsOn1() {
        val T0 = TaskSpec(id = "T0", preConditions = listOf(DependencyDefinition("E1")))
        assertTrue ( T0.dependsOn(setOf("E1")) )
    }

    @Test fun dependsOn2() {
        val T1 = TaskSpec(id = "T1", preConditions = listOf(DependencyDefinition("E2")))
        val T0 = TaskSpec(id = "T0", subTasks = setOf(T1))
        assertTrue ( T0.dependsOn(setOf("E2")) )
    }

    @Test fun dependsOn3() {
        val T2 = TaskSpec(id = "T2")
        val T1 = TaskSpec(id = "T1", subTasks = setOf(T2), preConditions = listOf(DependencyDefinition("E2")))
        val T0 = TaskSpec(id = "T0", subTasks = setOf(T1))
        assertTrue ( T0.dependsOn(setOf("E2")) )
    }

    @Test fun collectDependency() {
        val T1 = TaskSpec(id = "T1", preConditions = listOf(DependencyDefinition("E1")))
        val T0 = TaskSpec(id = "T0", subTasks = setOf(T1))
        val matched = T0.collectDependent("E1").toList()
        assertEquals(1, matched.size)
        val (theOne) = matched
        assertEquals(T1, theOne)
    }

    /**
     * Two dependencies mapped to a single task
     */
    @Test fun collectDependency1() {
        val T1 = TaskSpec(id = "T1", preConditions = listOf(DependencyDefinition("E1")))
        val T2 = TaskSpec(id = "T2", preConditions = listOf(DependencyDefinition("E1")))
        val T0 = TaskSpec(id = "T0", subTasks = setOf(T1, T2))
        val matched = T0.collectDependent("E1").toList()
        assertEquals(2, matched.size)
        val (t1, t2) = matched
        assertEquals(T1, t1)
        assertEquals(T2, t2)
    }

    /**
     * Two layer of dependencies
     */
    @Test fun collectDependency2() {
        val T11 = TaskSpec(id = "T11", preConditions = listOf(DependencyDefinition("E1")))
        val T1 = TaskSpec(id = "T1", subTasks = setOf(T11))
        val T0 = TaskSpec(id = "T0", subTasks = setOf(T1))
        val matched = T0.collectDependent("E1").toList()
        assertEquals(1, matched.size)
        val (t11) = matched
        assertEquals(T11, t11)
    }

    /**
     * Single level hierarchy: a single level task to be matched against
     */
    @Test fun taskMatchHierarchyCreation() {
        val T0 = TaskSpec(id = "T0",
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition("E1")))
        val hierarchyMatch = TaskSpecMatch(root = T0, matched = setOf(T0))
        val attributes = mapOf("cobDate" to "20160101")
        val instance = hierarchyMatch.newHierarchy(attributes, setOf("E1"))
        assertEquals(T0, instance.taskSpec)
        assertEquals("20160101", instance["cobDate"]?.value)
        assertTrue ( instance.dependencyMeet("E1") )
    }

    /**
     * Two level hierarchy
     */

    @Test fun taskMatchHierarchyCreation1() {
        val T1 = TaskSpec(id = "T1",
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition("E1")))

        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1),
                attributes = mapOf(attrDef("cobDate", true)))

        val attributes = mapOf("cobDate" to "20160101")
        val hierarchyMatch = TaskSpecMatch(root = T0, matched = setOf(T1))
        val instance = hierarchyMatch.newHierarchy(attributes, setOf("E1"))
        assertEquals(T0, instance.taskSpec)
        assertEquals(1, instance.subTasks.size)
        assertEquals(T1, instance.subTasks[0].taskSpec)
    }

    /**
     * Two level hierarchy with two sub tasks and double match
     * \T0
     *  -\T1
     *  -|T2
     *  -\T3
     * No available instances
     *
     */
    @Test fun taskMatchHierarchyCreation2() {
        val T1 = TaskSpec(id = "T1",
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition("E1")))
        val T2 = TaskSpec(id = "T2",
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition("E1")))

        val T3 = TaskSpec(id = "T3",
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition("E3")))

        val T0 = TaskSpec(id = "T0",
                subTasks = setOf(T1, T2, T3),
                attributes = mapOf(attrDef("cobDate", true)))

        val attributes = mapOf("cobDate" to "20160101")
        val hierarchyMatch = TaskSpecMatch(root = T0, matched = setOf(T1, T2))
        val instance = hierarchyMatch.newHierarchy(attributes, setOf("E1"))
        assertEquals(T0, instance.taskSpec)
        assertEquals(2, instance.subTasks.size)
        val (t1, t2) = instance.subTasks
        assertEquals(T1, t1.taskSpec)
        assertEquals(T2, t2.taskSpec)
    }

    /**
     * Test path to a single root element
     */
    @Test fun testPath() {
        val t0 = TaskSpec(id = "t0")
        val path = t0.path(t0)
        assertEquals(1, path.size)
        assertEquals(t0, path[0])
    }

    /**
     * Test path to a node at the second level
     */

    @Test fun testPathDoubleLevel() {
        val t1 = TaskSpec(id = "t1")
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t1))
        val path = t0.path(t1)
        assertEquals(2, path.size)
        val (parent, child1) = path
        assertEquals(t0, parent)
        assertEquals(t1, child1)
    }

    /**
     * Test path to a node at the third level
     */

    @Test fun testPathThirdLevel() {
        val t2 = TaskSpec(id = "t2")
        val t1 = TaskSpec(id = "t1", subTasks = setOf(t2))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t1))

        val path = t0.path(t2)
        assertEquals(3, path.size)
        val (level0, level1, level2) = path
        assertEquals(t0, level0)
        assertEquals(t1, level1)
        assertEquals(t2, level2)
    }

    @Test fun testContainSub() {
        val t1 = TaskSpec(id = "T1")
        val t0 = TaskSpec(id = "T0", subTasks = setOf(t1))
        assertTrue ( t0.containSub(t1) )
        val t2 = TaskSpec(id = "T2")
        assertFalse ( t0.containSub(t2) )
    }
}
