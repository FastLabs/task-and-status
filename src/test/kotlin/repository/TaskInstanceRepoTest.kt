package repository


import orchestration.RouteTaskAction
import org.junit.Assert.*
import org.junit.Test
import task.*
import task.TaskStatus.COMPLETED
import task.TaskStatus.PENDING

class TaskInstanceRepoTest {

    @Test
    fun saveSingleTaskInstance() {
        val t0 = TaskSpec(id = "T0",
                attributes = mapOf(attrDef("cobDate", true)))
        val taskInstanceRepo = InMemoryTaskInstanceRepo()
        val t = t0.newTaskInstance()
        taskInstanceRepo.saveInstance(t).toBlocking().value()
        //assertions
        taskInstanceRepo.getTaskInstances(setOf(t0))
                .subscribe {
                    val (theOne) = it
                    assertEquals("T0", theOne.taskSpec.id)
                    assertNotNull(theOne.id)
                }
    }

    /**
     * Complex scenario of updating task hierarchy
     */
    @Test
    fun saveMultipleTimes() {
        val taskInstanceRepo = InMemoryTaskInstanceRepo()
        val t1 = TaskSpec(id = "t1", preConditions = listOf(DependencyDefinition("E1")))
        val t2 = TaskSpec(id = "t2", preConditions = listOf(DependencyDefinition("E2")))
        val t0 = TaskSpec(id = "t0", subTasks = setOf(t1, t2))

        val specMatch = TaskSpecMatch(root = t0, matched = setOf(t1))
        val hierarchy = specMatch.newHierarchy(matchDependencies = setOf("E1"))
        assertEquals("t0", hierarchy.id)
        assertEquals(1, hierarchy.subTasks.size)
        taskInstanceRepo.saveInstance(hierarchy).toBlocking().value()
        val existing = taskInstanceRepo.findTaskInstance("t0")
        assertEquals("t0", existing?.id)
        assertEquals(1, existing?.subTasks?.size)
        assertEquals(PENDING, existing?.subTasks?.get(0)?.status)
        taskInstanceRepo.saveInstance(hierarchy.subTasks[0].copy(status = COMPLETED)).toBlocking().value()
        val again = taskInstanceRepo.findTaskInstance("t1")
        assertEquals("t1", again?.id)
        assertEquals(COMPLETED, again?.status)

        //TODO: test when updating with a t2 instance when that does not exist - at the moment this is not supported,
        // not required as the hierarchy is filed with the pending tasks


    }

    @Test
    fun saveTaskHierarchy() {
        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))
        val taskInstanceRepo = InMemoryTaskInstanceRepo()
        val it0 = TaskInstance(id = "t0-inst", taskSpec = t0,
                subTasks = listOf(TaskInstance(id = "t1-inst", taskSpec = t1)))

        taskInstanceRepo.saveInstance(it0).toBlocking().value()
        val t1Instances = taskInstanceRepo.getTaskInstances(setOf(t1)).toBlocking().value()

        assertEquals(1, t1Instances.size)
        val (t1Instance0) = t1Instances
        assertEquals("T1", t1Instance0.taskSpec.id)
        assertNotNull(t1Instance0.id)


        taskInstanceRepo.getTaskInstances(setOf(t0))
                .subscribe({ t0Instances ->
                    assertEquals(1, t0Instances.size)
                    val (t0Instance0) = t0Instances
                    assertEquals("T0", t0Instance0.taskSpec.id)
                    assertNotNull(t0Instance0.id)
                    assertEquals(1, t0Instance0.subTasks.size)
                }, {
                    fail(it.message)
                })
    }

    /**
     * Initialise the repository with the following hierarchy
     * TO - cobDate: 20160101
     *  \-T1 - cobDate: 20160101
     *
     *  Matching against T1 with cobDate: 20160101 will return the full hierarchy
     */
    @Test
    fun matchTaskToHierarchy() {
        val taskInstanceRepo = InMemoryTaskInstanceRepo()


        val t1 = TaskSpec(id = "T1",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                attributes = mapOf(attrDef("region", true)))
        val t0 = TaskSpec(id = "T0",
                subTasks = setOf(t1),
                attributes = mapOf(attrDef("cobDate", true)))

        val it0 = TaskInstance(taskSpec = t0,
                id = "t0-inst",
                attributes = listOf(TaskAttribute(t0["cobDate"], "20160101")),
                subTasks = listOf(TaskInstance(taskSpec = t1
                        , id = "t1-inst"
                        , attributes = listOf(TaskAttribute(t1["cobDate"], "20160101")))))

        taskInstanceRepo.saveInstance(it0).toBlocking().value()

        val attributes = mapOf("cobDate" to "20160101")
        val match = TaskSpecMatch(root = t0, matched = setOf(t1))
        taskInstanceRepo.matchTaskInstanceHierarchies(setOf(match), attributes)
                .subscribe({ matched ->
                    assertEquals(1, matched.size)
                    val (first) = matched
                    assertEquals(t0, first.hierarchyRoot?.taskSpec) //found the root task
                }, { fail(it.message) })
    }


    @Test
    fun matchTaskNoHierarchy() {
        val taskInstanceRepo = InMemoryTaskInstanceRepo()
        val taskDef = TaskSpec(attributes = mapOf(attrDef("cobDate", true)), id = "UK-PARTY-SINGLE")

        val task = taskDef.newTaskInstance(
                attributes = mapOf("cobDate" to "20160202"))
        taskInstanceRepo.saveInstance(task)

        val attributes = mapOf("cobDate" to "20160202")
        val hierarchy = TaskSpecMatch(root = taskDef, matched = setOf())
        taskInstanceRepo.matchTaskInstanceHierarchies(setOf(hierarchy), attributes)
                .subscribe({ matched ->
                    assertEquals(1, matched.size)
                    assertEquals(taskDef, matched[0].hierarchyRoot?.taskSpec)
                }, { fail(it.message) })
    }
}


